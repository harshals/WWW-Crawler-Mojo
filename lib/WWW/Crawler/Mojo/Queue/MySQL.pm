package WWW::Crawler::Mojo::Queue::MySQL;
use strict;
use warnings;
use utf8;
use Mojo::Base 'WWW::Crawler::Mojo::Queue';
use Mojo::mysql;
use Storable qw(freeze thaw );

has debug => 0;
has table_name => 'jobs';
has 'jobs';
has blob => 1;


sub new {
    my ($class, $conn, %opts) = @_;
    my $self = $class->SUPER::new(jobs => Mojo::mysql->new($conn)->db, %opts);
    my $table = $self->table_name;

    unless ($self->query("exists")) { $self->query("create"); }
    
    return $self;
}

sub query {
	my $self = shift;
    my $type = shift;
    my $table = $self->table_name;

    my $result;
    
    eval { 
        if ($type eq "exists" )  {
            $result = $self->jobs->query("show tables LIKE '$table'" )->rows;
        }elsif ($type eq 'create') {
            $result = $self->jobs->query( "create table $table (id integer auto_increment primary key, digest varchar(255) , data blob, completed boolean , unique key digested(digest, completed))");
        }elsif ($type eq 'find') {
            $result = $self->jobs->query("select id from $table where digest = ?", shift)->hash;
        }elsif ($type eq 'dequeue') {
	    	my $tx = $self->jobs->begin;
            $result = $self->jobs->query("select id, data from $table where completed = 0 order by id asc limit 1 for update" )->hash;
            $self->jobs->query("update $table set completed = 1 where id = ?", $result->{id}) if $result->{id};
            $tx->commit;
        }elsif ($type eq 'enqueue') {
            my $job = shift;
            $result = $self->jobs->query("insert into $table (digest, data, completed) values(?,?,?)", $job->digest, $self->serialize($job), 0 );
        }elsif ($type eq 'delete') {
            $result = $self->jobs->query("delete from $table where completed = 1 and digest = ?", shift) ;
        }elsif ($type eq 'next') {
            $result = $self->jobs->query("select data from $table where completed = 0 order by id limit ? ",  shift )->arrays;
        }elsif ($type eq 'count') {
            $result = $self->jobs->query("select count(id) AS total from $table where completed = 0")->hash->{total};
        }

    };
    warn "Mysql Error: " , $@ if $@;
    return $result;
}

sub empty {
	my $self = shift;
	my $table = $self->table_name;
	$self->jobs->query( "delete from $table");
}
has redundancy => sub { 
    
    my $self = shift;
    return sub {
        my $job = shift;
        my $table = $self->table_name;
        my $result = $self->query("find", $job->digest);
        #my $result = $self->jobs->query("select id from $table where digest = ?", $job->digest)->hash;
        $self->debug and $result and warn "found duplicate for " , $job->url, " \@ ", $result->{id}, "\n" ;
        return ($result) ? $result->{id} : undef;
    } 
};

sub serialize {
    my ($self, $job) = @_;
    my $frozen = ($self->blob) ? freeze($job) : $job->url->to_string;
    if (length $frozen > 65534) {
        warn "truncating job ", $job->url ,"\n"; 
        return $self->serialize( WWW::Crawler::Mojo::Job->new( url => $job->url )) ;
    }
    return $frozen;
}

sub deserialize {
    return shift->blob ? thaw(shift) : WWW::Crawler::Mojo::Job->new( url => Mojo::URL->new(shift) );

    #my ($self, $data) = @_;
    #my $r = $self->blob ? thaw($data) : WWW::Crawler::Mojo::Job->new( url => Mojo::URL->new($data) );
    #$self->blob and !$r and warn "error in thaw , data exceeded blob size, passing empty job\n";
    #return ($r) ? $r : WWW::Crawler::Mojo::Job->new( url => Mojo::URL->new("http://www.example.com") );
}
sub reset {
    my $self = shift;
    my $table = $self->table_name;
	$self->jobs->query("update $table set completed = 0");
}
sub dequeue {
    my $self = shift;
    my $table = $self->table_name;

	my $last = $self->query("dequeue");
    #eval {
    #	my $tx = $self->jobs->begin;
    #	$last = $self->jobs->query("select id, data from $table where completed = 0 order by id asc limit 1 for update" )->hash;
    #	$self->jobs->query("update $table set completed = 1 where id = ?", $last->{id}) if $last->{id};
    #	$tx->commit;
    #};
    #warn "error in dequeue is ",  $@ if $@;

	$self->debug and $last->{id} and warn "dequeueing " , $last->{id}, " with url ",  $self->deserialize($last->{data})->url , "\n";

    return ($last->{id}) ? $self->deserialize($last->{data}) :  undef;
}

sub enqueue {
    shift->_enqueue(@_);
}

sub length {
    my $self = shift;
    my $table = $self->table_name;

    return $self->query("count");
    #return $self->jobs->query("select count(id) AS total from $table where completed = 0")->hash->{total};
}

sub next {
    my ($self, $offset, $future) = @_;
    $offset = $offset || 0;
    my $table = $self->table_name;
    my $tx = $self->query("next", $offset + 1);
    #my $tx = $self->jobs->query("select data from $table where completed = 0 order by id limit ? ", $offset + 1 )->arrays;
    return ( scalar(@$tx) eq ($offset+1) ) ?  $self->deserialize($tx->[ $offset ]->[0]) : undef;
}

sub requeue {
    my ($self, $job ) = @_;

    $self->_enqueue($job, 1);
}

sub shuffle { }

sub _enqueue {
    my ($self, $job, $requeue) = @_;
    my $table = $self->table_name;
	my $is_redundant = $self->redundancy->($job) || 0;

	$self->debug and !$is_redundant and warn "enqueing " , $job->url , "\n";
	$self->debug and $is_redundant and !$requeue and warn "rejecting duplicate " , $job->url , "\n";
	$self->debug and $is_redundant and $requeue and warn "requeueing " , $job->url , "\n";
    return if (!$requeue && $is_redundant);
    #my $tx = $self->jobs->begin;
        my ($result, $can_insert) = (undef , 1);
        if ($requeue) {
            $result = $self->query("delete", $job->digest);
            #$result = $self->jobs->query("delete from $table where completed = 1 and digest = ?", $job->digest) ;
            $can_insert = $result->affected_rows;
            $self->debug and !$result->affected_rows and warn "Can't requeue!found a pending job for ", $job->url, "\n";
        }
        return unless $can_insert;
        #$result = $self->jobs->query("insert into $table (digest, data, completed) values(?,?,?)", $job->digest, $self->serialize($job), 0 );
        $result = $self->query("enqueue", $job);
		$self->debug and $result->affected_rows eq 1 and warn "added  ", $job->url , " \@ ", $result->last_insert_id  ,"\n";
        #$tx->commit;
    return $self;
}

1;
