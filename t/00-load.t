#!/usr/bin/env perl -w

use common::sense;
use lib::abs '../lib';
use Test::More tests => 2;
use Test::NoWarnings;

BEGIN {
	use_ok( 'AnyEvent::DBD::mysql' );
}

diag( "Testing AnyEvent::DBD::mysql $AnyEvent::DBD::mysql::VERSION, Perl $], $^X" );
