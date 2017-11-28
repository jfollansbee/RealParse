#! /usr/bin/perl -w

use diagnostics;
use lib '/usr/local/lib/perl5/5.6.1/i686-linux/'; # for Date::Calc
use DirHandle;
use Time::localtime;
use Date::Manip qw(ParseDate UnixDate);

# Inititalize variables
$TheLine = "";
@logentry = {};
$logdir = "/usr/local/rmserver/Logs";
$parsefilename = "None";

# Get today's date
$today = localtime;
($year, $month, $day) = ($today->year+1900, $today->mon+1, $today->mday);
$today = ($year . "/" . $month . "/" . $day);

# Get the current RealServer log
@filelist = &logDirfiles($logdir);	# Get the list of rmaccess.log files from the Logs directory
foreach $i (@filelist) {
	$modifytime = (stat($i))[9];	# Get the last modified time for the file
	$tm = localtime($modifytime);	# Create an object that lets us get the date info
	($modifyyear, $modifymonth, $modifyday) = ($tm->year+1900, $tm->mon+1, $tm->mday);
	$modifytime = ($modifyyear . "/" . $modifymonth . "/" . $modifyday);
	if ($modifytime eq $today) { $parsefilename = $i }	# See  if a log file exists for today.
}
print "Today is $today.\n";
print "Parsefilename is $parsefilename.\n";

$parsefilename = "/usr/local/rmserver/Logs/rmaccess.log.20021013090338";

if ($parsefilename ne "None") {

	open(LOG, $parsefilename) || die("Can't open rmserver log to insert into the rmserver log database.");

	while(<LOG>) {
		$TheLine = $_;
		chomp($TheLine);
		my ($client_ip_address, $identuser, $authuser, $datetime, $gmt_offset, $method, $filename, $protocol, $status_code, $bytes_sent, $client_info, $client_GUID, $stat1, $stat2, $stat3, $file_size, $file_time, $sent_time, $resends, $failed_resends, $stream_components, $start_time, $server_address, $average_bitrate, $packets_sent, $presentation_id);
		@logentry = split(/ /,$TheLine);
#		@logentry2 = split(/\[/,$TheLine);
		$client_ip_address = $logentry[0];
		$identuser = $logentry[1];
		$authuser = $logentry[2];
		# Sets $1, $2, $3 for the date, time, gmt_offset in the timestamp.
		$TheLine =~ /([\d][\d]\/[A-Z][a-z][a-z]\/[\d][\d][\d][\d]):(\d+:\d+:\d+) -([\d][\d][\d][\d])/;
		# Have the subroutine format the data correctly for insertion into the database
		$datetime = &formatDate($1, $2);
		$gmt_offset = $3;
		# Get the method, filename, and protocol
		$TheLine =~ /"(\S+) (.*?) (\S+)"/;
		($method, $filename, $protocol) = ($1, quotemeta($2), quotemeta($3));
		$status_code = $logentry[9];
		$bytes_sent = $logentry[10];
		$TheLine =~ m/(\S*)/;	# Get the client IP address at beginning of the line
		$client_ip_address = $1;
#		@space_matches = ($TheLine =~ m/(\s\d+)/g);	# Find all numeric entries after spaces
#		print "$space_matches[0] $space_matches[1] $space_matches[2] $space_matches[3] $space_matches[4] $space_matches[5] $space_matches[6] $space_matches[7]\n";
#		@bracket_matches = ($TheLine =~ m/(\[[^]]*\])/g);	# Find all entries between brackets
#		print "$bracket_matches[0], $bracket_matches[1], $bracket_matches[2], $bracket_matches[3]\n";
#		print "$client_ip_address $identuser $authuser $datetime $gmt_offset $method $filename\n";
#		print "$protocol $status_code $bytes_sent $client_info\n";
		}

	close(LOG);

}

# $position = 0;
# foreach $j (@logentry2) {
# 	print "$position: $j\n";
#	$position++
#	} 

# open(MAIL, "|/usr/sbin/sendmail -t") || die("Could not open sendmail for mailing.");

# print MAIL "To: joef\@compelinteractive.com\n";
# print MAIL "Subject: RealServer Log Parser Activity\n";
# print MAIL "Today is $today.\n";
# print MAIL "This RealServer log file was parsed: $parsefilename.\n";

# close(MAIL);

# Gets the rmaccess.log.* file names from the RealServer Logs directory
sub logDirfiles {
	my $dir = shift(@_);
	my $dh = DirHandle->new($dir) or die("Can't open $dir: $!");
	return sort
#		grep { -f }
		grep { /rmaccess/ }
		map { "$dir/$_" }
		grep { !/^\./ }
		$dh->read();
} # end sub logDirfiles

# Formats the date properly for insertion into the database
sub formatDate {
	my ($logdate, $logtime, $mydate, $year, $month, $day, $mydatetime);
	$logdate = shift(@_);
	$logtime = shift(@_);
	$mydate = ParseDate($logdate);
	($year, $month, $day) = UnixDate($mydate, "%Y", "%m", "%d");
	$mydatetime = ($year . "-" . $month . "-" . $day . " " . $logtime);
	return $mydatetime;
} # end sub formatDate
