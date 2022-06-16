#!/usr/bin/env perl

#
# Handler for running a
#

use strict;
use warnings;

use CGI qw(-debug);
use Digest::SHA qw(hmac_sha256_hex);
use feature qw(say);

print "Content-type: text/plain\n\n";

my $secret = $ENV{"SECRET"};
if (not defined $secret) {
    die "No secret set in environment! Aborting...\n";
}

my $sync_dir = $ENV{"SYNC_DIR"};
if (not defined $sync_dir) {
    die "No sync directory in environment (SYNC_DIR)! Aborting...\n";
}
elsif (!-e $sync_dir) {
    die "Invalid sync directory '$sync_dir'. Aborting...\n";
}

my $q = CGI->new;
if (my $data = $q->param('POSTDATA')) {

    my $check = $ENV{"HTTP_X_HUB_SIGNATURE_256"};
    my $signature = "sha256=" . hmac_sha256_hex($data, $secret);

    if ($signature ne $check) {
        die "Mismatching secret key! Aborting...\n";
    }

    chdir $sync_dir;
    system("git", "pull");
    my @files = glob("*.gpkg");
    my @cmd = ("geoserver-sync", "--user", "admin", "--workspace", "ehri", @files);
    say join(" ", @cmd);
    if (system(@cmd)) {
        die "Sync script exited with non-zero code: $!\n";
    }
}

