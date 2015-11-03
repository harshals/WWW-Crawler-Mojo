use strict;
use warnings;
use Test::More;
use Test::Mojo;
use utf8;
use Data::Dumper;
use Mojo::IOLoop;
use File::Basename 'dirname';
use File::Spec::Functions qw{catdir splitdir rel2abs canonpath};
use lib catdir(dirname(__FILE__), '../lib');
use lib catdir(dirname(__FILE__), 'lib');


use WWW::Crawler::Mojo;
use WWW::Crawler::Mojo::Job;
use WWW::Crawler::Mojo::Queue::MySQL;
use Data::Printer;
use Test::More ;


my $url = 'http://www.eformz.in/';
#my $url = 'http://www.diamonds.net/';

my $bot = WWW::Crawler::Mojo->new;

$bot->queue(WWW::Crawler::Mojo::Queue::MySQL->new($ENV{TEST_ONLINE}, table_name => 'test'));
$bot->queue->empty;
$bot->queue->debug(0);
$bot->queue->blob(1);
$bot->enqueue( WWW::Crawler::Mojo::Job::upgrade("WWW::Crawler::Mojo::Job", $url));

is($bot->queue->length, 1, 'begin');
my %errors;

$bot->on('res' => sub {
    my ($bot, $scrape, $job, $res) = @_;
    return unless $res->code == 200;
    warn "scraping ", $job->url, "\n";
    for my $job ($scrape->()) {
        $bot->enqueue($job) if $job->url =~ m/contact/ix;
    };
});

$bot->on(error => sub {
        my ($bot, $error, $job) = @_;
        warn " requeing on error: $_[1] \n";
        $errors{ $job->digest}++ ;
        $bot->requeue($job) if $errors{ $job->digest} < 2;
});

$bot->init;
Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

is($bot->queue->length, 0, 'end');


done_testing;
