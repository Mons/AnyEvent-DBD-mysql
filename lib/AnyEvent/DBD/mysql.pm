package AnyEvent::DBD::mysql;

use uni::perl ':dumper';
use 5.008008; # don't use old crap without utf8
use common::sense 3;m{
	use strict;
	use warnings;
}x;
use Scalar::Util 'weaken';
use Carp;
use DBI;
use DBD::mysql;
use AE 5;
use Time::HiRes 'time', 'sleep';

=head1 NAME

AnyEvent::DBD::mysql - ...

=cut

our $VERSION = '0.01'; $VERSION = eval($VERSION);

=head1 SYNOPSIS

    package Sample;
    use AnyEvent::DBD::mysql;

    ...

=head1 DESCRIPTION

    ...

=cut


=head1 METHODS

=over 4

=item ...()

...

=back

=cut

sub new {
	my ($pkg,$dsn,$user,$pass,$args,@args) = @_;
	$args ||= {};
	my $self = bless {@args},$pkg;
	$self->{cnn} = [$dsn,$user,$pass,$args];
	$self->{id} = sprintf '%08x', int $self unless defined $self->{id};
	$self->{queue_size} = 2 unless defined $self->{queue_size};
	$self->{reconnect_interval} = 5 unless defined $self->{reconnect_interval};
	$self->{queue} = [];
	#$self->{current};
	#$self->connect;
	$self->{querynum}  = 0;
	$self->{queuetime} = 0;
	$self->{querytime} = 0;
	$self;
}

BEGIN {
	no strict 'refs';
	for my $method (qw( cnn queue_size debug )) {
		*$method = sub { @_ > 1 ? $_[0]->{$method} = $_[1] : $_[0]->{$method} }
	}
}

sub callcb($$@) {
	my ($cb,$err,@args) = @_;
	my $e;
	my $wa = wantarray;
	my ($rc,@rc);
	{
		local $@;
		eval {
			$@ = $err;
			$wa ? @rc = $cb->(@args) : $rc = $cb->(@args);
		1 } or $e = $@;
	}
	die $e if defined $e;
	$wa ? @rc : $rc;
}

sub connect {
	my $self = shift;
	my ($dsn,$user,$pass,$args) = @{ $self->{cnn} };
	local $args->{RaiseError} = 0;
	local $args->{PrintError} = 0;
	local $args->{mysql_auto_reconnect} = 1;
	
	warn "Connecting to $dsn" if  $self->{debug} > 2;
	if( $self->{db} = DBI->connect($dsn,$user,$pass,$args) ) {
		#warn "connect $dsn $user {@{[ %$args  ]}} successful ";
		$self->{fh} = $self->{db}->mysql_fd + 0;
		#warn "socket = $self->{fh}";
		#exit 255;
		warn "Connection to $dsn established. fd = $self->{fh}\n" if $self->{debug} > 2;
		$self->{fh} > 0 or die "Database fh not defined";
		
		$self->{lasttry} = undef;
		$self->{gone} = undef;
		return $self->{db}->ping;
	} else {
		#warn "connect $dsn $user {@{[ %$args  ]}} failed ";
		$self->{error} = DBI->errstr;
		$self->{gone} = time unless defined $self->{gone};
		$self->{lasttry} = time;
		warn "Connection to $dsn failed: ".DBI->errstr;
		return 0;
	}
}

our %METHOD = (
	selectrow_array    => 'fetchrow_array',
	selectrow_arrayref => 'fetchrow_arrayref',
	selectrow_hashref  => 'fetchrow_hashref',
	selectall_arrayref => sub {
		my ($st,$args) = @_;
		$st->fetchall_arrayref($args->{Slice});
	}, #'fetchall_arrayref',
	selectall_hashref  => 'fetchall_hashref',
	selectcol_arrayref => sub {
		my ($st,$args) = @_;
		$st->fetchall_arrayref($args->{Columns});
	},
	execute            => sub { $_[0]; }, # just pass the $st
);

sub DESTROY {}

sub _dequeue {
	my $self = shift;
	if ($self->{db}->{pg_async_status} == 1 ) {	
		warn "Can't dequeue (), while processing query ($self->{current}[0]) by @{[ (caller)[1,2] ]}";
		if ( @{ $self->{queue} } ) {
			warn "\tHave queue $self->{queue}[0][1]  $self->{queue}[0][2]";
		}
		return;
	}
	#warn "Run dequeue with status=$self->{db}->{pg_async_status}";
	return $self->{current} = undef unless @{ $self->{queue} };
	my $next = shift @{ $self->{queue} };
	my $at = shift @$next;
	$self->{queuetime} += time - $at;
	my $method = shift @$next;
	local $self->{queuing} = 0;
	$self->$method(@$next);
}

sub begin_work {
	my $self = shift;
	my $cb = pop;
	$self->execute("begin",$cb);
}

sub commit {
	my $self = shift;
	my $cb = pop || sub {};
	$self->execute("commit",$cb);
}

sub rollback {
	my $self = shift;
	my $cb = pop || sub {};
	$self->execute("rollback",$cb);
}

our $AUTOLOAD;
sub  AUTOLOAD {
	
	my ($method) = $AUTOLOAD =~ /([^:]+)$/;
	my $self = shift;
	die sprintf qq{Can't locate autoloaded object method "%s" (%s) via package "%s" at %s line %s.\n}, $method, $AUTOLOAD, ref $self, (caller)[1,2] # '
		unless exists $METHOD{$method};
	my $fetchmethod = $METHOD{$method};
	defined $fetchmethod or croak "Method $method not implemented yet";
	ref (my $cb = pop) eq 'CODE' or croak "need callback";
	
	$self->{db} or $self->connect or return callcb( $cb,$self->{error} );
	
	if ($self->{current}) {
		if ( $self->{queue_size} > 0 and @{ $self->{queue} } > $self->{queue_size} - 1 ) {
			my $c = 1;
			my $counter = ++$self->{querynum};
			printf STDERR "\e[036;1m$self->{id}/Q$counter\e[0m. [\e[03${c};1m%0.4fs\e[0m] < \e[03${c};1m%s\e[0m > ".("\e[031;1mQuery run out of queue size $self->{queue_size}\e[0m")."\n", 0 , $_[0];
			return callcb( $cb, "Query $_[0] run out of queue size $self->{queue_size}" );
		} else {
			#warn "Query $_[0] pushed to queue because of current=$self->{current}[0]\n" if $self->{debug} > 1;
			push @{ $self->{queue} }, [time(), $method, @_,$cb];
			return;
		}
	}
	my $query = shift;
	my $args = shift || {};
	$args->{async} = 1;
	my $counter = ++$self->{querynum};
	warn "prepare call <$query>( @_ )" if $self->{debug} > 2;
	$self->{current} = [$query,@_];
	$self->{current_start} = time();
	
	#weaken $self;
	$self or warn("self was destroyed"),return;
	my ($st,$w,$t,$check);
	my @watchers;
	push @watchers, sub {
		$self and $st or warn("no self"), @watchers = (), return 1;
		if ($st->mysql_async_ready) {
			undef $w;
			my $res = $st->mysql_async_result;
			my $run = time - $self->{current_start};
			$self->{querytime} += $run;
			my ($diag,$DIE);
			if ($self->{debug}) {
				$diag = $self->{current}[0];
				my @bind = @{ $self->{current} };
				shift @bind;
				$diag =~ s{\?}{ "'".shift(@bind)."'" }sge;
			} else {
				$diag = $self->{current}[0];
			}
			if (!$res) {
				$DIE = $self->{db}->errstr;
			}
			local $self->{qd} = $diag;
			if ($self->{debug}) {
				my $c = $run < 0.01 ? '2' : $run < 0.1 ? '3' : '1';
				my $x = $DIE ? '1' : '6';
				printf STDERR "\e[036;1m$self->{id}/Q$counter\e[0m. [\e[03${c};1m%0.4fs\e[0m] < \e[03${x};1m%s\e[0m > ".($DIE ? "\e[031;1m$DIE\e[0m" : '')."\n", $run , $diag;
			}
			local $self->{queuing} = @{ $self->{queue} };
			if ($res) {
				undef $@;
				if (ref $fetchmethod) {
					$cb->($res, $st->$fetchmethod($args));
				} else {
					$cb->($res, $st->$fetchmethod);
				}
				undef $st;
				undef $self->{current};
				$self->_dequeue();
				@watchers = ();
			} else {
				$st->finish;
				# TODO: if in transaction?
				if ($DIE =~ /Lost connection to MySQL server/ and time - $self->{lasttry} > $self->{reconnect_interval}) {
					undef $st;
					my $cur = delete $self->{current};
					my $query = shift @$cur;
					@watchers = ();
					if( $self->connect ) {
						warn "Query $cur->[0] unshifted to queue with '$method' because of reconnect\n" if $self->{debug} > 1;
						unshift @{ $self->{queue} }, [time(), $method, $query, $args, @$cur, $cb];
						$self->_dequeue();
						return;
					}
					
				}
				callcb($cb,$DIE);
				undef $st;
				undef $self->{current};
				@watchers = ();
				$self->_dequeue();
			}
			return 1;
		}
		return 0;
		#undef $w;
	};
	if (0 and $query =~ /^\s*begin(?:_work|)\s*$/i) {
		my $rc = eval { $self->{db}->begin_work;1 };
		$rc or warn;
		
			my ($diag,$DIE);
			if (!$rc) {
				$DIE = $@;
			}
			if ($self->{debug}) {
				$diag = $self->{current}[0];
				my @bind = @{ $self->{current} };
				shift @bind;
				$diag =~ s{\?}{ "'".shift(@bind)."'" }sge;
			} else {
				$diag = $self->{current}[0];
			}
			local $self->{qd} = $diag;
			if ($self->{debug}) {
				my $c = 2;
				my $x = $DIE ? '1' : '6';
				printf STDERR "\e[036;1m$self->{id}/Q$counter\e[0m. [\e[03${c};1m%0.4fs\e[0m] < \e[03${x};1m%s\e[0m > ".($DIE ? "\e[031;1m$DIE\e[0m" : '')."\n", 0 , $diag;
			}
		
		undef $self->{current};
		if($rc) {
			$cb->( '0E0' );
		} else {
			$cb->();
		}
		$self->_dequeue if @{ $self->{queue} };
		return;
	}
	
	$st = $self->{db}->prepare($query,$args)
		and $st->execute(@_) 
		or return do{
			undef $st;
			@watchers = ();
			
			#local $@ = $self->{db}->errstr;
			warn $self->{db}->errstr;
			callcb($cb, $self->{db}->errstr);
			
			$self->_dequeue;
		};
	#warn time()." call query $query done";
	# At all we don't need timers for the work, but if we have some bugs, it will help us to find them
	push @watchers, AE::timer 1,1, $watchers[0];
	push @watchers, AE::io $self->{fh}, 0, $watchers[0];
	$watchers[0]() and return;
	return;
}

=head1 AUTHOR

Mons Anderson, C<< <mons@cpan.org> >>

=head1 COPYRIGHT & LICENSE

Copyright 2011 Mons Anderson, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

=cut

1;
