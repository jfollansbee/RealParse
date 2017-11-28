#! /usr/bin/perl -w

# Parses Caudium logs for info on "pseudo-streamed" files

use diagnostics;
use DirHandle;
use Time::localtime;
use Date::Manip qw(ParseDate UnixDate);
use DBI;

# Inititalize variables
$TheLine = "";
$logdir = "/usr/local/caudium/logs/hms_surprise";
$parsefilename = "";
$server_type = "0";	# Server types are 0 => web server, 1 => RealServer

# Set the database connection variables
DELETED

# Get today's date
$today = localtime;
($year, $month, $day) = ($today->year+1900, $today->mon+1, $today->mday);
$today = ($year . "/" . $month . "/" . $day);

# Get the current RealServer log
my @filelist = &logDirfiles($logdir);	# Get the list of log files from the logs directory
my @parsefilelist = ($filelist[($#filelist - 1)], $filelist[$#filelist]);	# We only need the last two

print "Today is $today.\n";

# Connect to the database
$dbh = DBI->connect("DBI:mysql:$database:$host",$username,$password) || die("Could not connect: $DBI::errstr");

my ($query, $sth);

# Lock the tables
$query = qq{ LOCK TABLES access WRITE, file WRITE, client WRITE, stats_mask1 WRITE, stats_mask2 WRITE, stats_mask3 WRITE, network WRITE, components WRITE };
$sth = $dbh->prepare($query); $sth->execute; $sth->finish;


# Get the time in seconds since the last database entry
$query = qq{ SELECT UNIX_TIMESTAMP(MAX(datetime)) AS lastentryGMT FROM access WHERE logging_style IS NULL };     # Get the last datetime, excluding RealServer log entries.
$sth = $dbh->prepare($query);
$sth->execute;
my $maxdatetime = $sth->fetchrow_hashref;
$sth->finish;
$lastentryGMT = $maxdatetime->{ lastentryGMT };

foreach $i (@parsefilelist) {
 print "$i\n";
 $parsefilename = $i;
 print "Parsefilename is $i\n";

if ($parsefilename ne "None") {
	open(LOG, $parsefilename) || die("Can't open Caudium server log for parsing!");
		while(<LOG>) {
			$TheLine = $_;
			chomp($TheLine);
			if ($TheLine =~ m/\.wma/ || m/\.wmv/) {
				# Get the client IP address at beginning of the line
				$TheLine =~ m/(\S*)/;
				$client_ip_address = $1;
				# Set these to follow the log format
				my ($identuser, $authuser) = ("-", "-");
				# Find all numeric entries after spaces
				@space_matches = ($TheLine =~ m/(\s\d+)/g);
				# Get rid of the leading spaces
				foreach $i (@space_matches) { $i =~ s/ //; }
				# Put values into variables.
				my ($status_code, $bytes_sent) = (@space_matches);
				# Find all entries between brackets
				my(@bracket_matches) = ($TheLine =~ m/(\[[^]]*\])/g);
				# Get rid of the opening and closing brackets
				foreach $i (@bracket_matches) { $i =~ s/\[//; } 
				foreach $i (@bracket_matches) { $i =~ s/\]//; } 
				# Sets $1, $2, $3 for the date, time, gmt_offset in the timestamp.
				$bracket_matches[0] =~ /([\d][\d]\/[A-Z][a-z][a-z]\/[\d][\d][\d][\d]):(\d+:\d+:\d+) -([\d][\d][\d][\d])/;
				# Have the subroutine format the data correctly
				my ($datetime, $logepoch) = &formatDate($1, $2);
				my ($gmt_offset) = $3;

				# Insert data if new log entry later than last database entry.
				if ($logepochGMT > $lastentryGMT)
				{

#					# Insert the IP address and timestamp into the database
					my ($query) = qq{ INSERT INTO access VALUES ( NULL, ?, ?, ?, ?, ?, NULL, NULL, ? ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $client_ip_address);
					$sth->bind_param(2, $identuser);
					$sth->bind_param(3, $authuser);
					$sth->bind_param(4, $datetime);
					$sth->bind_param(5, $gmt_offset);
					$sth->bind_param(6, $server_type);
					$sth->execute;
					$sth->finish;

					# Get the access_id value for later use
					$query = qq{ SELECT max(access_id) as max_id from access };
					$sth = $dbh->prepare($query);
					$sth->execute;
					my $access_max_id = $sth->fetchrow_arrayref;	# Global for use elsewhere
					$sth->finish;

					# Get the method, filename, protocol and user-agent (client_info)
					$TheLine =~ /"(\S+) (.*?) (\S+)" \d\d\d \d+ "-" "(.*?)"/;
					my ($method, $filename, $protocol_version, $client_info) = ($1, $2, $3, $4);
				
					@pathfile = split(/\//, $filename);
					$name = $pathfile[$#pathfile];  # The filename is final item
					$name =~ m/(^.+\.\w*)/;
					$name = $1;
					pop(@pathfile); # Remove the file name from the list.
					$path = join('/', @pathfile);

					# Insert the file information
					$query = qq{ INSERT INTO file VALUES ( NULL, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, @$access_max_id ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $method);
					$sth->bind_param(2, $path);
					$sth->bind_param(3, $name);
					$sth->bind_param(4, $protocol_version);
					$sth->bind_param(5, $status_code);
					$sth->bind_param(6, $bytes_sent);
					$sth->execute;
					$sth->finish;

					# Insert the client info and GUID via a subroutine
					$query = qq{ INSERT INTO client VALUES ( NULL, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @$access_max_id ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $client_info);
					$sth->execute;
					$sth->finish;
					print "Data inserted: $filename\n";
					} else {
					print "No data inserted.\n";
					} # end if logepoch > lastentry statement
				} # end if $TheLine matches wma or wmv statement
			} # end while (<LOG>) statement
	close(LOG);
} # end if parsefilename statement

} # end foreach statement

$query = qq{ UNLOCK TABLES };
$sth = $dbh->prepare($query); $sth->execute; $sth->finish;

$dbh->disconnect;

# open(MAIL, "|/usr/sbin/sendmail -t") || die("Could not open sendmail for mailing.");

# print MAIL "To: joef\@compelinteractive.com\n";
# print MAIL "Subject: RealServer Log Parser Activity\n";
# print MAIL "Logging style is $loggingstyle.\n";
# print MAIL "Today is $today.\n";
# print MAIL "This RealServer log file was parsed: $parsefilename.\n";

# close(MAIL);

# Gets the log.* file names from the Caudium logs directory
sub logDirfiles {
	my $dir = shift(@_);
	my $dh = DirHandle->new($dir) or die("Can't open $dir: $!");
	return sort
		grep { /log\./ }	# Get only the log.[date] files
		map { "$dir/$_" }
		grep { !/^\./ }
		$dh->read();
} # end sub logDirfiles

# Formats the date properly for insertion into the database
sub formatDate {
	my ($logdate, $logtime, $logyear, $logmonth, $logday, $loghours, $logminutes, $logseconds, $logdatetime, $logepoch);
	$logdate = shift(@_);
	$logtime = shift(@_);
	$logdatetime = ($logdate . ":" . $logtime);
	$logdatetime = ParseDate($logdatetime);
	($logyear, $logmonth, $logday, $loghours, $logminutes, $logseconds, $logepochGMT) = UnixDate($logdatetime, "%Y", "%m", "%d", "%H", "%M", "%S", "%s");
	$logdatetime = ($logyear . "-" . $logmonth . "-" . $logday . " " . $logtime);
	return ($logdatetime, $logepochGMT);
} # end sub formatDate
