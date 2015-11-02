use strict;
use warnings;
use utf8;
use File::Basename 'dirname';
use File::Spec::Functions qw{catdir splitdir rel2abs canonpath};
use lib catdir(dirname(__FILE__), '../lib');
use lib catdir(dirname(__FILE__), 'lib');
use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test' unless $ENV{TEST_ONLINE};

use WWW::Crawler::Mojo;
use WWW::Crawler::Mojo::Queue::MySQL;
use WWW::Crawler::Mojo::Job;
use Mojo::Util qw/md5_sum/;

my $queue = WWW::Crawler::Mojo::Queue::MySQL->new($ENV{TEST_ONLINE}, table_name => 'testing_jobs');
$queue->empty;
$queue->blob(0);
$queue->debug(1);

my $url = 'http://3aclothing.com/content-about-us';

my $job1 = WWW::Crawler::Mojo::Job->new;
$job1->url(Mojo::URL->new($url));
$queue->enqueue($job1);

is ref $queue->next, 'WWW::Crawler::Mojo::Job';
is $queue->next->url, $url;
is $queue->length, 1, 'right number 1';

my $job2 = WWW::Crawler::Mojo::Job->new;
$job2->url(Mojo::URL->new($url));
$queue->enqueue($job2);

is $queue->length, 1, 'right number 1';

$job1 = $queue->dequeue;

is $job1->digest , md5_sum($url), "digest is correct";
is $job1->url , $url, "1st job being processed";
is $queue->length, 0, 'queue length is 0';

$queue->requeue($job1);
is $queue->length, 1, 'requeue will increment queue by 1';

$queue->requeue($job1);
is $queue->length, 1, 'length should still be 1';
done_testing;
