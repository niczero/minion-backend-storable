package Minion::Backend::File;
use Minion::Backend -base;

our $VERSION = 2.011;

use IO::Compress::Gzip 'gzip';
use IO::Uncompress::Gunzip 'gunzip';
use List::Util 'first';
use Storable qw(freeze thaw);
use Sys::Hostname 'hostname';
use Time::HiRes qw(time usleep);

has deserialize => sub { \&_deserialize };
has 'file';
has serialize => sub { \&_serialize };

sub dequeue {
  my ($self, $id, $wait, $options) = @_;
  usleep $wait * 1000000 unless my $job = $self->_try($id, $options);
  return $job || $self->_try($id, $options);
}

sub enqueue {
  my ($self, $task) = (shift, shift);
  my $args    = shift // [];
  my $options = shift // {};

  my $guard = $self->_guard->_write;

  my $job = {
    args     => $args,
    created  => time,
    delayed  => $options->{delay} ? (time + $options->{delay}) : 1,
    id       => $guard->_id,
    priority => $options->{priority} // 0,
    queue    => $options->{queue} // 'default',
    retries  => 0,
    state    => 'inactive',
    task     => $task
  };
  $guard->_jobs->{$job->{id}} = $job;

  return $job->{id};
}

sub fail_job { shift->_update(1, @_) }

sub finish_job { shift->_update(0, @_) }

sub job_info { shift->_guard->_jobs->{shift()} }

sub list_jobs {
  my ($self, $offset, $limit, $options) = @_;

  my $guard = $self->_guard;
  my @jobs = sort { $b->{created} <=> $a->{created} } values %{$guard->_jobs};
  @jobs = grep { $_->{state} eq $options->{state} } @jobs if $options->{state};
  @jobs = grep { $_->{task} eq $options->{task} } @jobs   if $options->{task};

  return [grep {defined} @jobs[$offset .. ($offset + $limit - 1)]];
}

sub list_workers {
  my ($self, $offset, $limit) = @_;
  my $guard = $self->_guard;
  my @workers = map { $self->_worker_info($guard, $_->{id}) }
    sort { $b->{started} <=> $a->{started} } values %{$guard->_workers};
  return [grep {defined} @workers[$offset .. ($offset + $limit - 1)]];
}

sub new { shift->SUPER::new(file => shift) }

sub register_worker {
  my ($self, $id) = @_;
  my $guard = $self->_guard->_write;
  my $worker = $id ? $guard->_workers->{$id} : undef;
  unless ($worker) {
    $worker = {host => hostname, id => $guard->_id, pid => $$, started => time};
    $guard->_workers->{$worker->{id}} = $worker;
  }
  $worker->{notified} = time;
  return $worker->{id};
}

sub remove_job {
  my ($self, $id) = @_;
  my $guard = $self->_guard;
  delete $guard->_write->_jobs->{$id}
    if my $removed = !!$guard->_job($id, qw(failed finished inactive));
  return $removed;
}

sub repair {
  my $self = shift;

  # Check worker registry
  my $guard   = $self->_guard->_write;
  my $workers = $guard->_workers;
  my $minion  = $self->minion;
  my $after   = time - $minion->missing_after;
  $_->{notified} < $after and delete $workers->{$_->{id}} for values %$workers;

  # Abandoned jobs
  my $jobs = $guard->_jobs;
  for my $job (values %$jobs) {
    next if $job->{state} ne 'active' or $workers->{$job->{worker}};
    @$job{qw(finished result state)} = (time, 'Worker went away', 'failed');
  }

  # Old jobs
  $after = time - $minion->remove_after;
  delete $jobs->{$_->{id}}
    for grep { $_->{state} eq 'finished' && $_->{finished} < $after }
    values %$jobs;
}

sub reset { shift->_guard->_spurt({}) }

sub retry_job {
  my ($self, $id, $retries) = (shift, shift, shift);
  my $options = shift // {};

  my $guard = $self->_guard;
  return undef unless my $job = $guard->_job($id, 'failed', 'finished');
  return undef unless $job->{retries} == $retries;
  $guard->_write;
  ++$job->{retries};
  $job->{delayed} = time + $options->{delay} if $options->{delay};
  defined $options->{$_} and $job->{$_} = $options->{$_} for qw(priority queue);
  @$job{qw(retried state)} = (time, 'inactive');
  delete @$job{qw(finished result started worker)};

  return 1;
}

sub stats {
  my $self = shift;

  my $active = 0;
  my (%seen, %states);
  my $guard = $self->_guard;
  for my $job (values %{$guard->_jobs}) {
    ++$states{$job->{state}};
    ++$active if $job->{state} eq 'active' and not $seen{$job->{worker}}++;
  }

  return {
    active_workers   => $active,
    inactive_workers => keys(%{$guard->_workers}) - $active,
    active_jobs      => $states{active}   // 0,
    inactive_jobs    => $states{inactive} // 0,
    failed_jobs      => $states{failed}   // 0,
    finished_jobs    => $states{finished} // 0
  };
}

sub unregister_worker { delete shift->_guard->_write->_workers->{shift()} }

sub worker_info { $_[0]->_worker_info($_[0]->_guard, $_[1]) }

sub _deserialize {
  gunzip \(my $compressed = shift), \my $uncompressed;
  return thaw $uncompressed;
}

sub _guard { Minion::Backend::File::_Guard->new(backend => shift) }

sub _serialize {
  gzip \(my $uncompressed = freeze(pop)), \my $compressed;
  return $compressed;
}

sub _try {
  my ($self, $id, $options) = @_;

  my $guard = $self->_guard;
  my @ready = grep { $_->{state} eq 'inactive' } values %{$guard->_jobs};
  my %queues = map +($_ => 1), @{$options->{queues} || ['default']};
  my $tasks = $self->minion->tasks;

  @ready = grep +($queues{$_->{queue}} and $tasks->{$_->{task}}), @ready;
  @ready = sort { $a->{created} <=> $b->{created} } @ready;
  @ready = sort { $b->{priority} <=> $a->{priority} } @ready;

  my $now = time;
  return undef unless my $job = first { $_->{delayed} < $now } @ready;
  $guard->_write;
  @$job{qw(started state worker)} = (time, 'active', $id);
  return $job;
}

sub _update {
  my ($self, $fail, $id, $retries, $result) = @_;

  my $guard = $self->_guard;
  return undef unless my $job = $guard->_job($id, 'active');
  return undef unless $job->{retries} == $retries;

  $guard->_write;
  @$job{qw(finished result)} = (time, $result);
  $job->{state} = $fail ? 'failed' : 'finished';
  return 1;
}

sub _worker_info {
  my ($self, $guard, $id) = @_;

  return undef unless $id && (my $worker = $guard->_workers->{$id});
  my @jobs = map $_->{id},
      grep +($_->{state} eq 'active' and $_->{worker} eq $id),
      values %{$guard->_jobs};

  return {%$worker, jobs => \@jobs};
}

package Minion::Backend::File::_Guard;
use Mojo::Base -base;

use Fcntl ':flock';
use Digest::MD5 'md5_hex';
use Storable qw(retrieve store);

sub DESTROY {
  my $self = shift;
  $self->_spurt($self->_data) if $self->{write};
  flock $self->{lock}, LOCK_UN;
}

sub new {
  my $self = shift->SUPER::new(@_);
  $self->_spurt({}) unless -f (my $file = $self->{backend}->file);
  open $self->{lock}, '>', "$file.lock";
  flock $self->{lock}, LOCK_EX;
  return $self;
}

sub _data { $_[0]{data} //= retrieve($_[0]{backend}->file) }

sub _id {
  my $self = shift;
  my $id;
  do { $id = md5_hex(time . rand 999) }
    while $self->_workers->{$id} or $self->_jobs->{$id};
  return $id;
}

sub _job {
  my ($self, $id) = (shift, shift);
  return undef unless my $job = $self->_jobs->{$id};
  return(grep(+($job->{state} eq $_), @_) ? $job : undef);
}

sub _jobs { shift->_data->{jobs} //= {} }

sub _spurt { store($_[1] => $_[0]{backend}->file) }

sub _workers { shift->_data->{workers} //= {} }

sub _write { ++$_[0]{write} && return $_[0] }

1;
__END__

=encoding utf8

=head1 NAME

Minion::Backend::File - File backend

=head1 SYNOPSIS

  use Minion::Backend::File;

  my $backend = Minion::Backend::File->new('/some/path/minion.data');

=head1 DESCRIPTION

L<Minion::Backend::File> is a highly portable file-based backend for
L<Minion>.

=head1 ATTRIBUTES

L<Minion::Backend::File> inherits all attributes from L<Minion::Backend> and
implements the following new ones.

=head2 deserialize

  my $cb   = $backend->deserialize;
  $backend = $backend->deserialize(sub {...});

A callback used to deserialize data, defaults to using L<Storable> with gzip
compression.

  $backend->deserialize(sub {
    my $bytes = shift;
    return {};
  });

=head2 file

  my $file = $backend->file;
  $backend = $backend->file('/Users/sri/minion.data');

File all data is stored in.

=head2 serialize

  my $cb   = $backend->serialize;
  $backend = $backend->serialize(sub {...});

A callback used to serialize data, defaults to using L<Storable> with gzip
compression.

  $backend->serialize(sub {
    my $hash = shift;
    return '';
  });

=head1 METHODS

L<Minion::Backend::File> inherits all methods from L<Minion::Backend> and
implements the following new ones.

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, 0.5);
  my $job_info = $backend->dequeue($worker_id, 0.5, {queues => ['important']});

Wait for job, dequeue it and transition from C<inactive> to C<active> state or
return C<undef> if queue was empty.

These options are currently available:

=over 2

=item queues

  queues => ['important']

One or more queues to dequeue jobs from, defaults to C<default>.

=back

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item id

  id => '10023'

Job id.

=item retries

  retries => 3

Number of times job has been retried.

=item task

  task => 'foo'

Task name.

=back

=head2 enqueue

  my $job_id = $backend->enqueue('foo');
  my $job_id = $backend->enqueue(foo => [@args]);
  my $job_id = $backend->enqueue(foo => [@args] => {priority => 1});

Enqueue a new job with C<inactive> state.

These options are currently available:

=over 2

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item priority

  priority => 5

Job priority, defaults to C<0>.

=item queue

  queue => 'important'

Queue to put job in, defaults to C<default>.

=back

=head2 fail_job

  my $bool = $backend->fail_job($job_id, $retries);
  my $bool = $backend->fail_job($job_id, $retries, 'Something went wrong!');
  my $bool = $backend->fail_job($job_id, $retries, {msg => "Wrong, wrong!"});

Transition from C<active> to C<failed> state.

=head2 finish_job

  my $bool = $backend->finish_job($job_id, $retries);
  my $bool = $backend->finish_job($job_id, $retries, 'All went well!');
  my $bool = $backend->finish_job($job_id, $retries, {msg => 'All went well!'});

Transition from C<active> to C<finished> state.

=head2 job_info

  my $job_info = $backend->job_info($job_id);

Get information about a job or return C<undef> if job does not exist.

  # Check job state
  my $state = $backend->job_info($job_id)->{state};

  # Get job result
  my $result = $backend->job_info($job_id)->{result};

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item created

  created => 784111777

Time job was created.

=item delayed

  delayed => 784111777

Time job was delayed to.

=item finished

  finished => 784111777

Time job was finished.

=item priority

  priority => 3

Job priority.

=item queue

  queue => 'important'

Queue name.

=item result

  result => 'All went well!'

Job result.

=item retried

  retried => 784111777

Time job has been retried.

=item retries

  retries => 3

Number of times job has been retried.

=item started

  started => 784111777

Time job was started.

=item state

  state => 'inactive'

Current job state, usually C<inactive>, C<active>, C<failed>, or C<finished>.

=item task

  task => 'foo'

Task name.

=item worker

  worker => '154'

Id of worker that is processing the job.

=back

=head2 list_jobs

  my $batch = $backend->list_jobs($offset, $limit);
  my $batch = $backend->list_jobs($offset, $limit, {state => 'inactive'});

Returns the same information as L</"job_info"> but in batches.

These options are currently available:

=over 2

=item state

  state => 'inactive'

List only jobs in this state.

=item task

  task => 'test'

List only jobs for this task.

=back

=head2 list_workers

  my $batch = $backend->list_workers($offset, $limit);

Returns the same information as L</"worker_info"> but in batches.

=head2 new

  my $backend = Minion::Backend::File->new('/some/path/minion.data');

Construct a new L<Minion::Backend::File> object.

=head2 register_worker

  my $worker_id = $backend->register_worker;
  my $worker_id = $backend->register_worker($worker_id);

Register worker or send heartbeat to show that this worker is still alive.

=head2 remove_job

  my $bool = $backend->remove_job($job_id);

Remove C<failed>, C<finished> or C<inactive> job from queue.

=head2 repair

  $backend->repair;

Repair worker registry and job queue if necessary.

=head2 reset

  $backend->reset;

Reset job queue.

=head2 retry_job

  my $bool = $backend->retry_job($job_id, $retries);
  my $bool = $backend->retry_job($job_id, $retries, {delay => 10});

Transition from C<failed> or C<finished> state back to C<inactive>.

These options are currently available:

=over 2

=item delay

  delay => 10

Delay job for this many seconds (from now).

=item priority

  priority => 5

Job priority.

=item queue

  queue => 'important'

Queue to put job in.

=back

=head2 stats

  my $stats = $backend->stats;

Get statistics for jobs and workers.

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister worker.

=head2 worker_info

  my $worker_info = $backend->worker_info($worker_id);

Get information about a worker or return C<undef> if worker does not exist.

  # Check worker host
  my $host = $backend->worker_info($worker_id)->{host};

These fields are currently available:

=over 2

=item host

  host => 'localhost'

Worker host.

=item jobs

  jobs => ['10023', '10024', '10025', '10029']

Ids of jobs the worker is currently processing.

=item notified

  notified => 784111777

Last time worker sent a heartbeat.

=item pid

  pid => 12345

Process id of worker.

=item started

  started => 784111777

Time worker was started.

=back

=head1 COPYRIGHT AND LICENCE

Copyright (c) 2014--2015 Sebastian Riedel.  All rights reserved.

Copyright (c) 2016 Nic Sandfield.  All rights reserved.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 SEE ALSO

L<Minion>, L<Minion::Backend::Pg>, L<Minion::Backend::SQLite>.
