#!/usr/bin/perl

use strict;
use warnings;

use AnyEvent;
use AnyEvent::DBD::mysql;

my ($dbname, $host, $port, $socket) = qw(aetest localhost 3308 /var/lib/mysql/mysql55.sock);
my $dsn = "DBI:mysql:database=$dbname;host=$host;port=$port;mysql_socket=$socket";

my ($user, $password) = qw(aetester passwd);

my $dbh = AnyEvent::DBD::mysql->new($dsn,$user,$password);

my $query = "SELECT col as value FROM (SELECT 1 col UNION SELECT 2 col) tblname";
$dbh->execute($query, sub { 
	unless (@_) { die sprintf("Error while executing query {%s}: %s", $query, $@); }

	my ($count, $sth) = (shift, shift);
	printf ("Last statement returned %d rows\n", $sth->rows);

	while (my $row = $sth->fetchrow_hashref ) {
		printf "Next value: %s\n" , $row->{value};
	}
});

# Result is:
# Last statement returned 2 rows
# Next value: 1
# Next value: 2
