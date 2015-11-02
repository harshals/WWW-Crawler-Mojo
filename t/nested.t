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
use WWW::Crawler::Mojo::Queue::MySQL;
use Data::Printer;
use Test::More ;


{
    package MockServer;
    use Mojo::Base 'Mojolicious';
    
    sub startup {
        my $self = shift;
        unshift @{$self->static->paths}, $self->home->rel_dir('public3');
    }
}

my $daemon = Mojo::Server::Daemon->new(
    app    => MockServer->new,
    ioloop => Mojo::IOLoop->singleton,
    silent => 1
);

$daemon->listen(['http://127.0.0.1'])->start;

my $port = Mojo::IOLoop->acceptor($daemon->acceptors->[0])->handle->sockport;
my $base = Mojo::URL->new("http://127.0.0.1:$port");
my $bot = WWW::Crawler::Mojo->new;

$bot->queue(WWW::Crawler::Mojo::Queue::MySQL->new($ENV{TEST_ONLINE}, table_name => 'test'));
$bot->queue->debug(1);
$bot->enqueue(WWW::Crawler::Mojo::resolve_href($base, '/index.html'));

my %errors;

$bot->on('res' => sub {
    my ($bot, $scrape, $job, $res) = @_;
    return unless $res->code == 200;
    warn "scraping ", $job->url, "\n";
    for my $job ($scrape->()) {
        $bot->enqueue($job);
    }
});

$bot->on(error => sub {
        my ($bot, $error, $job) = @_;
        warn " requeing on error: $_[1] \n";
        $errors{ $job->digest}++ ;
        $bot->requeue($job) if $errors{ $job->digest} < 2;
});

$bot->init;
Mojo::IOLoop->start unless Mojo::IOLoop->is_running;

is($bot->queue->length, 0, 'right length');


done_testing;
