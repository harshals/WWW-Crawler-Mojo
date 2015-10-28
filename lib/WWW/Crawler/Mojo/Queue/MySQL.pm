package WWW::Crawler::Mojo::Queue::MySQL;
use strict;
use warnings;
use utf8;
use Mojo::Base 'WWW::Crawler::Mojo::Queue';
use Mojo::mysql;
use Storable qw(freeze thaw );

has table_name => 'jobs';
has 'jobs';
has blob => 0;

sub new {
    my ($class, $conn, %opts) = @_;
    my $self = $class->SUPER::new(jobs => Mojo::mysql->new($conn)->db, %opts);
    my $table = $self->table_name;

    unless ($self->jobs->query("show tables LIKE '$table'" )->rows) {
        $self->jobs->query( "create table $table (id integer auto_increment primary key, digest varchar(255) , data blob, completed boolean , unique key digested(digest, completed))");
    }
    
    return $self;
}

sub empty {
	my $self = shift;
	my $table = $self->table_name;
	$self->jobs->query( "delete from $table");
}

has redundancy => sub {
    my %fix;
    
    return sub {
        my $d = $_[0]->digest;
        return 1 if $fix{$d};
        $fix{$d} = 1;
        return;
    };
};

sub serialize {
    return (shift->blob) ? freeze(shift) : shift->url->to_string;
}

sub deserialize {
    return shift->blob ? thaw(shift) : WWW::Crawler::Mojo::Job->new( url => Mojo::URL->new(shift) );
}
sub reset {
    my $self = shift;
    my $table = $self->table_name;
	$self->jobs->query("update $table set completed = 0");
}
sub dequeue {
    my $self = shift;
    my $table = $self->table_name;

	my $last;
	eval {
		my $tx = $self->jobs->begin;
		$last = $self->jobs->query("select id, data from $table where completed = 0 order by id asc limit 1 for update" )->hash;
		$self->jobs->query("update $table set completed = 1 where id = ?", $last->{id}) if $last->{id};
		$tx->commit;
	};

    return ($last->{id}) ? $self->deserialize($last->{data}) :  "";
}

sub enqueue {
    shift->_enqueue(@_);
}

sub length {
    my $self = shift;
    my $table = $self->table_name;

    return $self->jobs->query("select count(id) AS total from $table where completed = 0")->hash->{total};
}

sub next {
    my ($self, $offset, $future) = @_;
    my $table = $self->table_name;
    
    $offset = $offset || 0;

    my $d = $self->jobs->query("select data from $table where completed = 0 order by id limit ? ", $offset + 1 )->arrays->[ $offset ]->[0];

    return ($d) ? $self->deserialize($d)  : "";
}

sub requeue {
    my ($self, $job ) = @_;

    $self->_enqueue($job, 1);
}

sub shuffle { }

sub _enqueue {
    my ($self, $job, $requeue) = @_;
    my $table = $self->table_name;
    return if (!$requeue && $self->redundancy->($job));
	eval {
		my $tx = $self->jobs->begin;
		$self->jobs->query("delete from $table where completed = 1 and digest = ?", $job->digest) if $requeue;
		$self->jobs->query("insert into $table (digest, data, completed) values(?,?,?)", $job->digest, $self->serialize($job), 0 );
		$tx->commit;
	};
    return $self;
}

1;
