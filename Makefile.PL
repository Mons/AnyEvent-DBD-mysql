use 5.008008;
use ExtUtils::MakeMaker;
BEGIN{ $ENV{AUTHOR} and require CPAN::Meta and CPAN::Meta->import();  };

WriteMakefile(
	NAME              => 'AnyEvent-DBD-mysql',
	VERSION_FROM      => 'lib/AnyEvent/DBD/mysql.pm',
	PREREQ_PM         => {
		AnyEvent     => 5,
		'DBD::mysql' => 4.019,
	},
	ABSTRACT_FROM     => 'lib/AnyEvent/DBD/mysql.pm', # retrieve abstract from module
	AUTHOR            => 'Mons Anderson <mons@cpan.org>',
	LICENSE           => 'perl',
  BUILD_REQUIRES => {
        'lib::abs' => 0,
    },
	#PL_FILES          => { "Kit.pm.PL" => '$(INST_LIB)/AnyEvent/HTTP/Server/Kit.pm' },
	#clean             => { FILES => "Kit.pm" },
);
