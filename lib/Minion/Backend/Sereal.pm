package Minion::Backend::Sereal;
use Minion::Backend -base;

our $VERSION = 10.13;

use Sys::Hostname 'hostname';
use Time::HiRes ();
use List::Util qw(any none);

# Attributes

has 'file';

# Constructor

sub new { shift->SUPER::new(file => shift) }

# Methods

sub broadcast {
  my ($self, $command, $args, $ids) = (shift, shift, shift // [], shift // []);

  my $guard = $self->_guard->_write;
  my $inboxes = $guard->_inboxes;
  my $workers = $guard->_workers;
  @$ids = @$ids ? map exists($workers->{$_}), @$ids
      : keys %$workers unless @$ids;

  push @{$inboxes->{$_} //= []}, [$command, @$args] for @$ids;

  return !!@$ids;
}

sub dequeue {
  my ($self, $id, $wait, $options) = @_;
  return ($self->_try($id, $options) or do {
    Time::HiRes::usleep $wait * 1_000_000;
    $self->_try($id, $options);
  });
}

sub enqueue {
  my ($self, $task, $args, $options) = (shift, shift, shift // [], shift // {});

  my $guard = $self->_guard->_write;
  my $now = Time::HiRes::time;

  my $job = {
    args     => $args,
    attempts => $options->{attempts} // 1,
    created  => $now,
    delayed  => $now + ($options->{delay} // 0),
    expires  => defined($options->{expire}) ? $now + $options->{expire} : undef,
    lax      => $options->{lax} ? 1 : 0,
    id       => $guard->_job_id,
    notes    => $options->{notes} // {},
    parents  => $options->{parents} // [],
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

sub history {
  my $self = shift;
  my $guard = $self->_guard;
  my $jobs = $guard->_jobs;

  my @daily = ();
  my $now = Time::HiRes::time;
  push(@daily, { epoch => $now - 60 * 60 * $_, failed_jobs => 0, finished_jobs => 0 })
    for (reverse(1..24));
  my $min = $daily[0]->{epoch} + 1;
  for my $job (values %$jobs) {
    next if ($job->{finished} < $min);
    my $ix = int(($job->{finished} - $min) / 60 / 60);
    my $elem = $daily[$ix];
    ++$elem->{finished_jobs} if ($job->{state} eq 'finished');
    ++$elem->{failed_jobs}   if ($job->{state} eq 'failed');
  }
  return { daily => \@daily };
}

sub list_jobs {
  my ($self, $offset, $limit, $options) = @_;
  my $now   = Time::HiRes::time;
  my $guard = $self->_guard;
  my $jobs = $guard->_jobs;

  my @ids = (not defined $options->{ids}) ? keys %$jobs
      : (grep { defined && exists($jobs->{$_}) } @{$options->{ids}});
  @ids = sort { $b <=> $a } @ids;
  @ids = grep { $_ < $options->{before} } @ids
    if (defined $options->{before});

  my @jobs = grep {
      my $job = $_;
      ($job->{state} ne 'inactive' or not defined $job->{expires} or $job->{expires} > $now)
      and (not defined $options->{queues} or (any { $job->{queue} eq $_ } @{$options->{queues}}))
      and (not defined $options->{states} or (any { $job->{state} eq $_ } @{$options->{states}}))
      and (not defined $options->{tasks}  or (any { $job->{task}  eq $_ } @{$options->{tasks}}))
      and (not defined $options->{notes}  or (any { exists($job->{notes}->{$_}) } @{$options->{notes}}))
  } map { $jobs->{$_} } @ids;

  my $total = @jobs;
  $jobs = [ map {
          $_->{children} = $guard->_children($_->{id});
          $_->{time} = Time::HiRes::time;
          $_;
      } grep defined, @jobs[$offset .. ($offset + $limit - 1)]
  ];
  return { jobs => $jobs, total => $total };
}

sub list_locks {
  my ($self, $offset, $limit, $options) = @_;
  my $now   = Time::HiRes::time;
  my $guard = $self->_guard;
  my $locks = $guard->_locks;

  my @locks = ();
  for my $name (keys %$locks) {
    push(@locks, grep {
          my $lock = $_;
          $lock->{expires} > $now
          and (not defined $options->{names} or (any { $lock->{name} eq $_ } @{$options->{names}}))
      } map { { name => $name, expires => $_ // 0 } } @{$locks->{$name}});
  }
  # NOTE: Minion::Backend::Pg sorts by table id, we by name
  @locks = sort { $a->{name} cmp $b->{name} } @locks;

  my $total = @locks;
  $locks = [grep {defined} @locks[$offset .. ($offset + $limit - 1)]];
  return { locks => $locks, total => $total };
}

sub list_workers {
  my ($self, $offset, $limit, $options) = @_;
  my $guard = $self->_guard;
  my $workers = $guard->_workers;

  my @ids = (not defined $options->{ids}) ? keys %$workers
      : (grep { defined && exists($workers->{$_}) } @{$options->{ids}});
  @ids = sort { $b <=> $a } @ids;
  @ids = grep { $_ < $options->{before} } @ids
    if (defined $options->{before});

  my @workers = map { $self->_worker_info($guard, $_->{id}) }
    map { $workers->{$_} } @ids;

  my $total = @workers;
  $workers = [grep {defined} @workers[$offset .. ($offset + $limit - 1)]];
  return { workers => $workers, total => $total };
}

sub lock {
  my ($self, $name, $duration, $options) = (shift, shift, shift, shift // {});
  my $limit = $options->{limit} || 1;

  my $guard = $self->_guard->_write;
  my $locks = $guard->_locks->{$name} //= [];

  # Delete expired locks
  my $now = Time::HiRes::time;
  @$locks = grep +($now < ($_ // 0)), @$locks;

  # Check capacity
  return undef unless @$locks < $limit;
  return 1 unless $duration > 0;

  # Add lock, maintaining order
  my $this_expires = $now + $duration;

  push(@$locks, $this_expires) and return 1
    if ($locks->[$#$locks] // 0) < $this_expires;

  @$locks = sort { ($a // 0) <=> ($b // 0) } (@$locks, $this_expires);
  return 1;
}

sub note {
  my ($self, $id, $merge) = @_;
  my $guard = $self->_guard;
  return undef unless my $job = $guard->_write->_jobs->{$id};
  my %notes = ( %{$job->{notes}}, %$merge );
  delete @notes{ grep { not defined $notes{$_} } keys %$merge };
  $job->{notes} = \%notes;
  return 1;
}

sub receive {
  my ($self, $id) = @_;
  my $guard = $self->_guard->_write;
  my $inboxes = $guard->_inboxes;
  my $inbox = $inboxes->{$id} // [];
  $inboxes->{$id} = [];
  return $inbox;
}

sub register_worker {
  my ($self, $id, $options) = (shift, shift, shift // {});
  my $now = Time::HiRes::time;
  my $guard = $self->_guard->_write;
  my $worker = $id ? $guard->_workers->{$id} : undef;
  unless ($worker) {
    $worker = {host => hostname, id => $guard->_id, pid => $$, started => $now};
    $guard->_workers->{$worker->{id}} = $worker;
  }
  @$worker{qw(notified status)} = ($now, $options->{status} // {});
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
  my $minion = $self->minion;

  # Workers without heartbeat
  my $now     = Time::HiRes::time;
  my $guard   = $self->_guard->_write;
  my $workers = $guard->_workers;
  my $jobs    = $guard->_jobs;
  my $after   = $now - $minion->missing_after;
  $_->{notified} < $after and delete $workers->{$_->{id}} for values %$workers;

  # Old jobs with no unresolved dependencies and expired jobs
  $after = $now - $minion->remove_after;
  for my $job (values %$jobs) {
    next unless ($job->{state} eq 'finished' and $job->{finished} <= $after
            and none { $jobs->{$_}{state} ne 'finished' } @{$guard->_children($job->{id})})
        or ($job->{state} eq 'inactive' and defined $job->{expires} and $job->{expires} <= $now);
    delete $jobs->{$job->{id}};
  }

  # Jobs in queue without workers or not enough workers (cannot be retried and requires admin attention)
  for my $job (values %$jobs) {
    next unless $job->{state} eq 'inactive' and $job->{delayed} < $after;
    $job->{state} = 'failed';
    $job->{result} = 'Job appears stuck in queue';
  }

  # Jobs with missing worker (can be retried)
  my @abandoned = map [@$_{qw(id retries)}],
      grep +($_->{state} eq 'active' and $_->{queue} ne 'minion_foreground'
          and not exists $workers->{$_->{worker}}),
      values %$jobs;
  undef $guard;
  $self->fail_job(@$_, 'Worker went away') for @abandoned;
}

sub reset { shift->_guard->_write->_reset(@_); }

sub retry_job {
  my ($self, $id, $retries, $options) = (shift, shift, shift, shift // {});

  my $guard = $self->_guard;
  return undef
    unless my $job = $guard->_jobs->{$id};
  return undef unless $job->{retries} == $retries;
  my $now = Time::HiRes::time;
  $guard->_write;
  $job->{delayed} = $now + ($options->{delay} // 0);
  exists $options->{$_} and $job->{$_} = $options->{$_} for qw(attempts parents priority queue);
  exists $options->{lax}    and $job->{lax}     = $options->{lax} ? 1 : 0;
  exists $options->{expire} and $job->{expires} = $now + $options->{expire};
  @$job{qw(retried state)} = ($now, 'inactive');
  ++$job->{retries};

  return 1;
}

sub stats {
  my $self = shift;

  my ($inactive, $active, $delayed) = (0, 0, 0);
  my (%seen, %states);
  my $now   = Time::HiRes::time;
  my $guard = $self->_guard;
  for my $job (values %{$guard->_jobs}) {
    ++$states{$job->{state}};
    ++$inactive if $job->{state} eq 'inactive' and (not defined $job->{expires} or $job->{expires} > $now);
    ++$active   if $job->{state} eq 'active' and not $seen{$job->{worker}}++;
    ++$delayed  if $job->{state} eq 'inactive' and ($job->{delayed} > $now);
  }
  for my $lock (values %{$guard->_locks}) {
    defined and $_ > $now and ++$states{locks}
      for (@$lock);
  }

  return {
    active_workers   => $active,
    inactive_workers => keys(%{$guard->_workers}) - $active,
    active_jobs      => $states{active}   // 0,
    delayed_jobs     => $delayed,
    enqueued_jobs    => $guard->_job_count,
    failed_jobs      => $states{failed}   // 0,
    finished_jobs    => $states{finished} // 0,
    inactive_jobs    => $inactive,
    active_locks     => $states{locks} // 0,
    uptime           => 0 # uptime is always 0
  };
}

sub unlock {
  my ($self, $name) = @_;

  my $guard = $self->_guard->_write;
  my $locks = $guard->_locks->{$name} //= [];
  my $length = @$locks;
  my $now = Time::HiRes::time;

  my $i = 0;
  ++$i while $i < $length and ($locks->[$i] // 0) <= $now;
  return undef if $i >= $length;

  $locks->[$i] = undef;
  return 1;
}

sub unregister_worker {
  my ($self, $id) = @_;
  my $guard = $self->_guard->_write;
  delete $guard->_inboxes->{$id};
  delete $guard->_workers->{$id};
}

sub _guard { Minion::Backend::Sereal::_Guard->new(backend => shift) }

sub _try {
  my ($self, $id, $options) = @_;
  my $tasks = $self->minion->tasks;
  my %queues = map +($_ => 1), @{$options->{queues} // ['default']};

  my $now = Time::HiRes::time;
  my $guard = $self->_guard;
  my $jobs = $guard->_jobs;
  my @ready = sort { $b->{priority} <=> $a->{priority}
        || $a->{id} <=> $b->{id} }
    grep +($_->{state} eq 'inactive' and $queues{$_->{queue}}
        and $tasks->{$_->{task}} and $_->{delayed} <= $now
        and (not defined $_->{expires} or $_->{expires} > $now)
        and (not defined $options->{id} or $_->{id} eq $options->{id})),
    values %$jobs;

  my $job;
  CANDIDATE: for my $candidate (@ready) {
    $job = $candidate and last CANDIDATE
      unless my @parents = @{$candidate->{parents} // []};
    for my $parent (@parents) {
      next unless my $pjob = $jobs->{$parent};
      next CANDIDATE if $pjob->{state} eq 'active'
          or ($pjob->{state} eq 'failed' and not $candidate->{lax})
          or ($pjob->{state} eq 'inactive' and (not defined $pjob->{expires} or $pjob->{expires} > $now));
    }
    $job = $candidate;
  }

  return undef unless $job;
  $guard->_write;
  @$job{qw(started state worker)} = ($now, 'active', $id);
  return $job;
}

sub _update {
  my ($self, $fail, $id, $retries, $result) = @_;

  my $guard = $self->_guard;
  return undef unless my $job = $guard->_job($id, 'active');
  return undef unless $job->{retries} == $retries;

  $guard->_write;
  @$job{qw(finished result)} = (Time::HiRes::time, $result);
  $job->{state} = $fail ? 'failed' : 'finished';
  undef $guard;

  return $fail ? $self->auto_retry_job($id, $retries, $job->{attempts}) : 1;
}

sub _worker_info {
  my ($self, $guard, $id) = @_;

  return undef unless $id && (my $worker = $guard->_workers->{$id});
  my @jobs = map $_->{id},
      grep +($_->{state} eq 'active' and $_->{worker} eq $id),
      values %{$guard->_jobs};

  return {%$worker, jobs => \@jobs};
}

package Minion::Backend::Sereal::_Guard;
use Mojo::Base -base;

use Fcntl ':flock';
use Time::HiRes ();
use Sereal::Decoder 'sereal_decode_with_object';
use Sereal::Encoder 'sereal_encode_with_object';

sub DESTROY {
  my $self = shift;
  $self->_save($self->_data => $self->{backend}->file) if $self->{write};
  flock $self->{lock}, LOCK_UN;
}

sub new {
  my $self = shift->SUPER::new(@_);
  my $path = $self->{backend}->file;
  $self->_save({} => $path) unless -f $path;
  open $self->{lock}, '>', "$path.lock";
  flock $self->{lock}, LOCK_EX;
  return $self;
}

sub _children {
  my ($self, $id) = @_;
  my $children = [];
  for my $job (values %{$self->_jobs}) {
    push @$children, $job->{id} if grep +($_ eq $id), @{$job->{parents} // []};
  }
  return $children;
}

sub _data { $_[0]{data} //= $_[0]->_load($_[0]{backend}->file) }

sub _id {
  my $id = int(Time::HiRes::time * 100000);
  while ($_[0]->_workers->{$id}) { $id++ };
  return $id;
}

sub _inboxes { $_[0]->_data->{inboxes} //= {} }

sub _job {
  my ($self, $id) = (shift, shift);
  return undef unless my $job = $self->_jobs->{$id};
  return grep(($job->{state} eq $_), @_) ? $job : undef;
}

sub _job_count { $_[0]->_data->{job_count} //= 0 }

sub _job_id {
  my ($self) = @_;
  my $id = int(Time::HiRes::time * 100000);
  while ($self->_jobs->{$id}) { $id++ };
  ++$self->_data->{job_count};
  return $id;
}

sub _jobs { $_[0]->_data->{jobs} //= {} }

sub _load {
  my ($self, $path) = @_;
  my $decoder = $self->{backend}{_guard_decoder} //= Sereal::Decoder->new;

  # Borrowed from Mojo::File v7.33
  CORE::open my $file, '<', $path or die qq{Failed to open file ($path): $!};
  my ($payload, $ret) = ('', undef);
  while ($ret = sysread $file, my $buffer, 131072, 0) { $payload .= $buffer }
  die qq{Failed to read file ($path): $!} unless defined $ret;

  return sereal_decode_with_object $decoder, $payload;
}

sub _locks { $_[0]->_data->{locks} //= {} }

sub _save {
  my ($self, $content, $path) = @_;
  my $encoder = $self->{backend}{_guard_encoder} //= Sereal::Encoder->new;
  my $payload = sereal_encode_with_object $encoder, $content;

  # Borrowed from Mojo::File v7.33
  CORE::open my $file, '>', $path or die qq{Failed to open file ($path): $!};
  (syswrite($file, $payload) // -1) == length $payload
    or die qq{Failed to write file ($path): $!};
  return;
}

sub _workers { $_[0]->_data->{workers} //= {} }

sub _write { ++$_[0]{write} && return $_[0] }

sub _reset {
  my ($self, $options) = (@_);
  if ($options->{all}) { $_[0]{data} = {} }
  elsif ($options->{locks}) { $self->_data->{locks} = {} }
}

1;

=encoding utf8

=head1 NAME

Minion::Backend::Sereal - File backend for Minion job queues.

=head1 SYNOPSIS

  use Minion::Backend::Sereal;

  my $backend = Minion::Backend::Sereal->new('/some/path/minion.data');

=head1 DESCRIPTION

L<Minion::Backend::Sereal> is a highly portable file-based backend for
L<Minion>.  It is 2--3x as fast as the L<Storable|Minion::Backend::Storable>
backend.

This version supports Minion v7.01.

=head1 ATTRIBUTES

L<Minion::Backend::Sereal> inherits all attributes from L<Minion::Backend> and
implements the following new one.

=head2 file

  my $file = $backend->file;
  $backend = $backend->file('/some/path/minion.data');

File all data is stored in.

=head1 METHODS

L<Minion::Backend::Sereal> inherits all methods from L<Minion::Backend> and
implements the following new ones.

=head2 broadcast

  my $bool = $backend->broadcast('some_command');
  my $bool = $backend->broadcast('some_command', [@args]);
  my $bool = $backend->broadcast('some_command', [@args], [$id1, $id2, $id3]);

Broadcast remote control command to one or more workers.

=head2 dequeue

  my $job_info = $backend->dequeue($worker_id, 0.5);
  my $job_info = $backend->dequeue($worker_id, 0.5, {queues => ['important']});

Wait a given amount of time in seconds for a job, dequeue it and transition from
C<inactive> to C<active> state, or return C<undef> if queues were empty.

These options are currently available:

=over 2

=item id

  id => '10023'

Dequeue a specific job.

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

Job ID.

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

=item attempts

  attempts => 25

Number of times performing this job will be attempted, with a delay based on
L<Minion/"backoff"> after the first attempt, defaults to C<1>.

=item delay

  delay => 10

Delay job for this many seconds (from now), defaults to C<0>.

=item expire

  expire => 300

Job is valid for this many seconds (from now) before it expires. Note that this option is B<EXPERIMENTAL> and might
change without warning!

=item lax

  lax => 1

Existing jobs this job depends on may also have transitioned to the C<failed> state to allow for it to be processed,
defaults to C<false>. Note that this option is B<EXPERIMENTAL> and might change without warning!

=item notes

  notes => {foo => 'bar', baz => [1, 2, 3]}

Hash reference with arbitrary metadata for this job.

=item parents

  parents => [$id1, $id2, $id3]

One or more existing jobs this job depends on, and that need to have transitioned to the state C<finished> before it
can be processed.

=item priority

  priority => 5

Job priority, defaults to C<0>. Jobs with a higher priority get performed first.

=item queue

  queue => 'important'

Queue to put job in, defaults to C<default>.

=back

=head2 fail_job

  my $bool = $backend->fail_job($job_id, $retries);
  my $bool = $backend->fail_job($job_id, $retries, 'Something went wrong!');
  my $bool = $backend->fail_job(
    $job_id, $retries, {whatever => 'Something went wrong!'});

Transition from C<active> to C<failed> state with or without a result, and if there are attempts remaining, transition
back to C<inactive> with a delay based on L<Minion/"backoff">.

=head2 finish_job

  my $bool = $backend->finish_job($job_id, $retries);
  my $bool = $backend->finish_job($job_id, $retries, 'All went well!');
  my $bool = $backend->finish_job(
    $job_id, $retries, {whatever => 'All went well!'});

Transition from C<active> to C<finished> state with or without a result.

=head2 history

  my $history = $backend->history;

Get history information for job queue.

These fields are currently available:

=over 2

=item daily

  daily => [{epoch => 12345, finished_jobs => 95, failed_jobs => 2}, ...]

Hourly counts for processed jobs from the past day.

=back

=head2 list_jobs

  my $results = $backend->list_jobs($offset, $limit);
  my $results = $backend->list_jobs($offset, $limit, {states => ['inactive']});

Returns the information about jobs in batches.

  # Get the total number of results (without limit)
  my $num = $backend->list_jobs(0, 100, {queues => ['important']})->{total};

  # Check job state
  my $results = $backend->list_jobs(0, 1, {ids => [$job_id]});
  my $state = $results->{jobs}[0]{state};

  # Get job result
  my $results = $backend->list_jobs(0, 1, {ids => [$job_id]});
  my $result  = $results->{jobs}[0]{result};

These options are currently available:

=over 2

=item before

  before => 23

List only jobs before this id.

=item ids

  ids => ['23', '24']

List only jobs with these ids.

=item notes

  notes => ['foo', 'bar']

List only jobs with one of these notes.

=item queues

  queues => ['important', 'unimportant']

List only jobs in these queues.

=item states

  states => ['inactive', 'active']

List only jobs in these states.

=item tasks

  tasks => ['foo', 'bar']

List only jobs for these tasks.

=back

These fields are currently available:

=over 2

=item args

  args => ['foo', 'bar']

Job arguments.

=item attempts

  attempts => 25

Number of times performing this job will be attempted.

=item children

  children => ['10026', '10027', '10028']

Jobs depending on this job.

=item created

  created => 784111777

Epoch time job was created.

=item delayed

  delayed => 784111777

Epoch time job was delayed to.

=item expires

  expires => 784111777

Epoch time job is valid until before it expires.

=item finished

  finished => 784111777

Epoch time job was finished.

=item id

  id => 10025

Job id.

=item lax

  lax => 0

Existing jobs this job depends on may also have failed to allow for it to be processed.

=item notes

  notes => {foo => 'bar', baz => [1, 2, 3]}

Hash reference with arbitrary metadata for this job.

=item parents

  parents => ['10023', '10024', '10025']

Jobs this job depends on.

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

Epoch time job has been retried.

=item retries

  retries => 3

Number of times job has been retried.

=item started

  started => 784111777

Epoch time job was started.

=item state

  state => 'inactive'

Current job state, usually C<active>, C<failed>, C<finished> or C<inactive>.

=item task

  task => 'foo'

Task name.

=item time

  time => 78411177

Server time.

=item worker

  worker => '154'

Id of worker that is processing the job.

=back

=head2 list_locks

  my $results = $backend->list_locks($offset, $limit);
  my $results = $backend->list_locks($offset, $limit, {names => ['foo']});

Returns information about locks in batches.

  # Get the total number of results (without limit)
  my $num = $backend->list_locks(0, 100, {names => ['bar']})->{total};

  # Check expiration time
  my $results = $backend->list_locks(0, 1, {names => ['foo']});
  my $expires = $results->{locks}[0]{expires};

These options are currently available:

=over 2

=item names

  names => ['foo', 'bar']

List only locks with these names.

=back

These fields are currently available:

=over 2

=item expires

  expires => 784111777

Epoch time this lock will expire.

=item name

  name => 'foo'

Lock name.

=back

=head2 list_workers

  my $results = $backend->list_workers($offset, $limit);
  my $results = $backend->list_workers($offset, $limit, {ids => [23]});

Returns information about workers in batches.

  # Get the total number of results (without limit)
  my $num = $backend->list_workers(0, 100)->{total};

  # Check worker host
  my $results = $backend->list_workers(0, 1, {ids => [$worker_id]});
  my $host    = $results->{workers}[0]{host};

These options are currently available:

=over 2

=item before

  before => 23

List only workers before this id.

=item ids

  ids => ['23', '24']

List only workers with these ids.

=back

These fields are currently available:

=over 2

=item id

  id => 22

Worker id.

=item host

  host => 'localhost'

Worker host.

=item jobs

  jobs => ['10023', '10024', '10025', '10029']

Ids of jobs the worker is currently processing.

=item notified

  notified => 784111777

Epoch time worker sent the last heartbeat.

=item pid

  pid => 12345

Process id of worker.

=item started

  started => 784111777

Epoch time worker was started.

=item status

  status => {queues => ['default', 'important']}

Hash reference with whatever status information the worker would like to share.

=back

=head2 lock

  my $bool = $backend->lock('foo', 3600);
  my $bool = $backend->lock('foo', 3600, {limit => 20});

Try to acquire a named lock that will expire automatically after the given amount of time in seconds. An expiration
time of C<0> can be used to check if a named lock already exists without creating one.

These options are currently available:

=over 2

=item limit

  limit => 20

Number of shared locks with the same name that can be active at the same time, defaults to C<1>.

=back

=head2 new

  my $backend = Minion::Backend::Sereal->new('/some/path/minion.data');

Construct a new L<Minion::Backend::Sereal> object.

=head2 note

  my $bool = $backend->note($job_id, {mojo => 'rocks', minion => 'too'});

Change one or more metadata fields for a job. Setting a value to C<undef> will remove the field.

=head2 receive

  my $commands = $backend->receive($worker_id);

Receive remote control commands for worker.

=head2 register_worker

  my $worker_id = $backend->register_worker;
  my $worker_id = $backend->register_worker($worker_id);
  my $worker_id = $backend->register_worker(
    $worker_id, {status => {queues => ['default', 'important']}});

Register worker or send heartbeat to show that this worker is still alive.

These options are currently available:

=over 2

=item status

  status => {queues => ['default', 'important']}

Hash reference with whatever status information the worker would like to share.

=back

=head2 remove_job

  my $bool = $backend->remove_job($job_id);

Remove C<failed>, C<finished> or C<inactive> job from queue.

=head2 repair

  $backend->repair;

Repair worker registry and job queue if necessary.

=head2 reset

  $backend->reset({all => 1});

Reset job queue.

These options are currently available:

=over 2

=item all

  all => 1

Reset everything.

=item locks

  locks => 1

Reset only locks.

=back

=head2 retry_job

  my $bool = $backend->retry_job($job_id, $retries);
  my $bool = $backend->retry_job($job_id, $retries, {delay => 10});

Transition job back to C<inactive> state, already C<inactive> jobs may also be retried to change options.

These options are currently available:

=over 2

=item attempts

  attempts => 25

Number of times performing this job will be attempted.

=item delay

  delay => 10

Delay job for this many seconds (from now), defaults to C<0>.

=item expire

  expire => 300

Job is valid for this many seconds (from now) before it expires. Note that this option is B<EXPERIMENTAL> and might
change without warning!

=item lax

  lax => 1

Existing jobs this job depends on may also have transitioned to the C<failed> state to allow for it to be processed,
defaults to C<false>. Note that this option is B<EXPERIMENTAL> and might change without warning!

=item parents

  parents => [$id1, $id2, $id3]

Jobs this job depends on.

=item priority

  priority => 5

Job priority.

=item queue

  queue => 'important'

Queue to put job in.

=back

=head2 stats

  my $stats = $backend->stats;

Get statistics for the job queue.

These fields are currently available:

=over 2

=item active_jobs

  active_jobs => 100

Number of jobs in C<active> state.

=item active_locks

  active_locks => 100

Number of active named locks.

=item active_workers

  active_workers => 100

Number of workers that are currently processing a job.

=item delayed_jobs

  delayed_jobs => 100

Number of jobs in C<inactive> state that are scheduled to run at specific time in the future.

=item enqueued_jobs

  enqueued_jobs => 100000

Rough estimate of how many jobs have ever been enqueued.

=item failed_jobs

  failed_jobs => 100

Number of jobs in C<failed> state.

=item finished_jobs

  finished_jobs => 100

Number of jobs in C<finished> state.

=item inactive_jobs

  inactive_jobs => 100

Number of jobs in C<inactive> state.

=item inactive_workers

  inactive_workers => 100

Number of workers that are currently not processing a job.

=item uptime

  uptime => 1000

Uptime in seconds. Always 0.

=back

=head2 unlock

  my $bool = $backend->unlock('foo');

Release a named lock.

=head2 unregister_worker

  $backend->unregister_worker($worker_id);

Unregister worker.

=head1 COPYRIGHT AND LICENCE

Copyright (c) 2014 L<Sebastian Riedel|https://github.com/kraih>.

Copyright (c) 2015--2017 Sebastian Riedel & Nic Sandfield.

This program is free software, you can redistribute it and/or modify it under
the terms of the Artistic License version 2.0.

=head1 CONTRIBUTORS

=over 2

=item L<Manuel Mausz|https://github.com/manuelm>

=item L<Nils Diewald|https://github.com/Akron>

=back

=head1 SEE ALSO

L<Minion>, L<Minion::Backend::Storable>, L<Minion::Backend::SQLite>.
