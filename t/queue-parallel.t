use strict;
use warnings;
use utf8;
use File::Basename 'dirname';
use File::Spec::Functions qw{catdir splitdir rel2abs canonpath};
use lib catdir(dirname(__FILE__), '../lib');
use lib catdir(dirname(__FILE__), 'lib');
use Test::More;
use Data::Dumper;
use feature 'say';

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use WWW::Crawler::Mojo;
use WWW::Crawler::Mojo::Job;
use WWW::Crawler::Mojo::Queue::MySQL;
use Mojo::IOLoop;

my $queue = WWW::Crawler::Mojo::Queue::MySQL->new($ENV{TEST_ONLINE});
$queue->empty;

my %completed;
Mojo::IOLoop->delay(sub { 
	my $delay =  shift;
	foreach my $i (1..10) { 
	
		my $end = $delay->begin;
		Mojo::IOLoop->timer( 0 => sub {
			my $job = WWW::Crawler::Mojo::Job->new;
			$job->url(Mojo::URL->new("http://example.com/$i"));
			$queue->enqueue($job);
			say STDERR "enquing $i";
			$end->();
		});
	}		
	say STDERR 'queue is getting full' ;
}, sub {
	my $delay =  shift;
	for (1..10) {
		my $end = $delay->begin;
		Mojo::IOLoop->timer( 0 => sub {
			say STDERR "queue length is ", $queue->length;
			my $url = $queue->dequeue->url;
			say STDERR (exists $completed{ $url }) ?  " repeat $url " : " processing $url";
			$end->();
		});
	}
	say STDERR 'queue is getting empty' 
}, sub {
	say STDERR "queue length is ", $queue->length;
})->wait;

done_testing;
