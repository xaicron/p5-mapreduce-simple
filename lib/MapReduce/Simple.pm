package MapReduce::Simple;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Carp;
use Exporter qw(import);
use Class::Load qw(load_class);

use MapReduce::Simple::Job::Mapper;
use MapReduce::Simple::Job::Reducer;

our $VERSION       = '0.01';
our $JOB_PREFIX    = 'MapReduce::Simple::Job';
our $WORKER_PREFIX = 'MapReduce::Simple::Worker';
our $MAPPER_CLASS  = $JOB_PREFIX.'::Mapper';
our $REDUCER_CLASS = $JOB_PREFIX.'::Reducer';
our @EXPORT_OK     = qw(mapper reducer);
our %EXPORT_TAGS   = ( all => [@EXPORT_OK] );

__PACKAGE__->mk_accessors(qw/jobs/);

sub new {
    my ( $class, %args ) = @_;
    $class->SUPER::new( +{ jobs => $args{jobs} || [], } );
}

sub add_job {
    my ( $self, $job ) = @_;
    push( @{ $self->{jobs} }, $job );
    $self;
}

sub run {
    my ( $self, $data ) = @_;

    for my $job ( @{ $self->{jobs} } ) {
        if ( $job->isa('MapReduce::Simple::Job::Mapper') ) {
            $data = $self->run_mapper( $job, $data );
        }
        else {
            $data = $self->run_reducer( $job, $data );
        }
    }

    return $data;
}

sub run_mapper {
    my ( $self, $mapper, $data ) = @_;
    my $worker = $self->create_worker( $mapper->worker );
    return $worker->run_mapper( $mapper, $data );
}

sub run_reducer {
    my ( $self, $reducer, $data ) = @_;
    my $worker = $self->create_worker( $reducer->worker );
    return $worker->run_reducer( $reducer, $data );
}

sub create_worker {
    my ( $self, $worker_config ) = @_;
    my $worker_class = $worker_config->{class};
    _ensure_class_loaded($worker_class, $WORKER_PREFIX)->new( $worker_config->{args} );
}

sub mapper (&;%) {
    my ( $code, %opts ) = @_;
    my $mapper_class = delete $opts{mapper_class} || $MAPPER_CLASS;
    _ensure_class_loaded($mapper_class, $JOB_PREFIX)->new( $code, \%opts );
}

sub reducer (&;%) {
    my ( $code, %opts ) = @_;
    my $reducer_class = delete $opts{reducer_class} || $REDUCER_CLASS;
    _ensure_class_loaded($reducer_class, $JOB_PREFIX)->new( $code, \%opts );
}

sub _ensure_class_loaded {
    my ( $class, $prefix ) = @_;

    unless ( $class =~ s/^\+// || $class =~ /^$prefix/ ) {
        $class = "$prefix\::$class";
    }

    load_class($class);

    return $class;
}

1;
__END__

=head1 NAME

MapReduce::Simple -

=head1 SYNOPSIS

  use MapReduce::Simple;

=head1 DESCRIPTION

MapReduce::Simple is

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
