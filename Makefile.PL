use inc::Module::Install;
name 'MapReduce-Simple';
all_from 'lib/MapReduce/Simple.pm';

requires 'parent';
requires 'Class::Accessor::Fast';
requires 'DBD::SQLite';
requires 'DBI';
requires 'Parallel::ForkManager';
requires 'Try::Tiny';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::Exception';
test_requires 'Test::LoadAllModules';

auto_set_repository;
auto_include;
WriteAll;
