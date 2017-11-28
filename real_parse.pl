#! /usr/bin/perl -w

# Parses RealServer logs
# Note: This program assumes the stats logging style is "5".

use diagnostics;
use DirHandle;
use Time::localtime;
use Date::Manip qw(ParseDate UnixDate);
use DBI;

# Inititalize variables
$TheLine = "";
$logdir = "/usr/local/helix/Logs";
$parsefilename = "";
$server_type = "1";	# Server types are 0 => web server, 1 => RealServer

# Set the database connection variables
DELETED

# Get today's date
$today = localtime;
($year, $month, $day) = ($today->year+1900, $today->mon+1, $today->mday);
$today = ($year . "/" . $month . "/" . $day);

# Get the current RealServer log
my @filelist = &logDirfiles($logdir);	# Get the list of rmaccess.log files from the Logs directory
my @parsefilelist = ($filelist[($#filelist - 1)], $filelist[$#filelist]);	# We only need the last two


# Get the logging and stats_mask style
($logging_style, $stats_mask) = &logStyle;

print "Today is $today.\n";
print "Logging style is $logging_style\n";
print "Stats mask is $stats_mask\n";

# Connect to the database
$dbh = DBI->connect("DBI:mysql:$database:$host",$username,$password) || die("Could not connect: $DBI::errstr");
my ($query, $sth);

# Lock the tables
$query = qq{ LOCK TABLES access WRITE, file WRITE, client WRITE, stats_mask1 WRITE, stats_mask2 WRITE, stats_mask3 WRITE, network WRITE, components WRITE };
$sth = $dbh->prepare($query); $sth->execute; $sth->finish;

# Get the time in seconds since the last database entry
$query = qq{ SELECT UNIX_TIMESTAMP(MAX(datetime)) AS lastentryGMT FROM access WHERE logging_style IS NOT NULL }; 	# Get the last datetime, excluding web server log entries.
$sth = $dbh->prepare($query);
$sth->execute;
my $maxdatetime = $sth->fetchrow_hashref;
$sth->finish;
$lastentryGMT = $maxdatetime->{ lastentryGMT };

foreach $i (@parsefilelist) {
 print "Parsefilename is $i.\n";
 $parsefilename = $i;

if ($logging_style == 5) {
	if ($parsefilename ne "None") {
		open(LOG, $parsefilename) || die("Can't open logfile: $parsefilename for parsing!");
			while(<LOG>) {
				$TheLine = $_;
				chomp($TheLine);
				# Get the client IP address at beginning of the line
				$TheLine =~ m/(\S*)/;
				$client_ip_address = $1;
				# Set these to follow the log format
				my ($identuser, $authuser) = ("-", "-");
				# Find all numeric entries after spaces
				@space_matches = ($TheLine =~ m/(\s\d+)/g);
#				# Look for a chance that the first item in space_matches might be a directory that starts with a number. Start with a six to avoid accidentally removing an HTTP status_code
				if ($space_matches[0] =~ m/[6_9]\w+/) { shift(@space_matches);  }
				# Get rid of the leading spaces
				foreach $i (@space_matches) { $i =~ s/ //; }
				my $spl = $#space_matches;	# Find the length of the space matches array
				# print "SPL: $spl\n";
				# Put values into variables. NOTE: "start_time" not in this logging style.
				my ($status_code, $bytes_sent, $file_size, $file_time, $sent_time, $resends, $failed_resends, $presentation_id) = ($space_matches[0], $space_matches[1], $space_matches[($spl - 5)], $space_matches[($spl - 4)], $space_matches[($spl - 3)], $space_matches[($spl - 2)], $space_matches[($spl - 1)], $space_matches[$spl]);
				# print join(', ', @space_matches);
				# print "\nFIX: $status_code, $bytes_sent, $file_size, $file_time, $sent_time, $resends, $failed_resends, $presentation_id\n";
				# Find all entries between brackets
				my(@bracket_matches) = ($TheLine =~ m/(\[[^]]*\])/g);
				# Get rid of the opening and closing brackets
				foreach $i (@bracket_matches) { $i =~ s/\[//; } 
				foreach $i (@bracket_matches) { $i =~ s/\]//; } 
				# Sets $1, $2, $3 for the date, time, gmt_offset in the timestamp.
				$bracket_matches[0] =~ /([\d][\d]\/[A-Z][a-z][a-z]\/[\d][\d][\d][\d]):(\d+:\d+:\d+) -([\d][\d][\d][\d])/;
				# Have the subroutine format the data correctly
				my ($datetime, $logepochGMT) = &formatDate($1, $2);
				my ($gmt_offset) = $3;

				# Insert data if new log entry later than last database entry.
				if ($logepochGMT > $lastentryGMT) {

					# Insert the IP address and timestamp into the database
					my ($query) = qq{ INSERT INTO access VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, ? ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $client_ip_address);
					$sth->bind_param(2, $identuser);
					$sth->bind_param(3, $authuser);
					$sth->bind_param(4, $datetime);
					$sth->bind_param(5, $gmt_offset);
					$sth->bind_param(6, $logging_style);
					$sth->bind_param(7, $stats_mask);
					$sth->bind_param(8, $server_type);
					$sth->execute;
					$sth->finish;

					# Get the access_id value for later use
					$query = qq{ SELECT max(access_id) as max_id from access };
					$sth = $dbh->prepare($query);
					$sth->execute;
					my $access_max_id = $sth->fetchrow_arrayref;	# Global for use elsewhere
					$sth->finish;

					# Get the method, filename, and protocol
					$TheLine =~ /"(\S+) (.*?) (\S+)"/;
					my ($method, $filename, $protocol_version) = ($1, $2, $3);

					# Separate the path and filename
					my (@pathfile, $name, $path);
					if (($filename ne "/") && ($filename ne "")) {	# Suppress uninitialized variable error in $name
						@pathfile = split(/\//, $filename);
						$name = $pathfile[$#pathfile];	# The filename is final item
						$name =~ m/(^.+\.\w*)/;	# WARN: Arguments, i.e., "&target=", will be lost
						$name = $1;
						pop(@pathfile);	# Remove the file name from the list.
						$path = join('/', @pathfile);
						} else {
						($path, $name) = ("", "");
						}

					# Insert the file information
					$query = qq{ INSERT INTO file VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, @$access_max_id ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $method);
					$sth->bind_param(2, $path);
					$sth->bind_param(3, $name);
					$sth->bind_param(4, $protocol_version);
					$sth->bind_param(5, $status_code);
					$sth->bind_param(6, $bytes_sent);
					$sth->bind_param(7, $file_size);
					$sth->bind_param(8, $file_time);
					$sth->bind_param(9, $sent_time);
#					$sth->bind_param(00, $start_time);	# Placeholder for future use
					$sth->bind_param(10, $presentation_id);
					$sth->execute;
					$sth->finish;

					# Get the file_id value for later use
					$query = qq{ SELECT max(file_id) as max_id from file };
					$sth = $dbh->prepare($query);
					$sth->execute;
					$file_max_id = $sth->fetchrow_arrayref;	# Global for use elsewhere
					$sth->finish;

					my ($client_info, $client_GUID) = ($bracket_matches[1], $bracket_matches[2]);

					# Parse and insert the client info and GUID via a subroutine
					&parseClientInfo($client_info, $client_GUID, @$access_max_id);

					# Piggyback the ids on the bracket_matches array to maintain the scoping
					push(@bracket_matches, @$access_max_id);
					push(@bracket_matches, @$file_max_id);
					# Parse the stats mask info in a subroutine
					&parseStatsMask(@bracket_matches);

					# Insert the network conditions information
					$query = qq{ INSERT INTO network VALUES ( NULL, ?, ?, NULL, NULL, NULL, @$access_max_id, @$file_max_id ) };
					$sth = $dbh->prepare($query);
					$sth->bind_param(1, $resends);
					$sth->bind_param(2, $failed_resends);
#					$sth->bind_param(00, $server_address);	# Placeholder for future use
#					$sth->bind_param(00, $packets_sent);	# Placeholder for future use
#					$sth->bind_param(00, $average_bitrate);	# Placeholder for future use
					$sth->execute;
					$sth->finish;
					print "Data inserted: $filename\n";
					} else {
					print "No data inserted.\n";
					} # end if logepoch > lastentry statement

				} # end while (<LOG>) statement
		close(LOG);
	} # end if parsefilename statement
} else {
	print "Logging style is not \"5\". Abort!\n";
} # end if loggingstyle statement

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

# Gets the rmaccess.log.* file names from the RealServer Logs directory
sub logDirfiles {
	my $dir = shift(@_);
	my $dh = DirHandle->new($dir) or die("Can't open $dir: $!");
	return sort
		grep { /rmaccess/ }
		map { "$dir/$_" }
		grep { !/^\./ }
		$dh->read();
} # end sub logDirfiles

sub logStyle {
my ($logging_style, $stats_mask) = ("", "");
open(CFG, "/usr/local/helix/rmserver.cfg") || die("Can't open rmserver.cfg to get logging style.");
	while(<CFG>) {
		my($cfgline) = $_;
		chomp($cfgline);
		# Look for the line "<Var LoggingStyle="x"/>" in the rmserver.cfg file
		if ($cfgline =~ m/LoggingStyle=\"([1-5])\"/) {
			$logging_style = $1;
		}
		# Look for the line <Var StatsMask="x"/> in the rmserver.cfg file
		if ($cfgline =~ m/StatsMask=\"([1-7])\"/) {
			$stats_mask = $1;
		}
	}
close(CFG);
return ($logging_style, $stats_mask);
} # end sub logStyle

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

# Parses the client_info field and inserts the data, plus the client_GUID
sub parseClientInfo {
	my ($client_info, $client_GUID, @client_info_elements, $i, $platform, $os_version, $client_version, $type, $distribution, $language, $cpu, $embedded, $access_id);
	($client_info, $client_GUID, $access_id) = @_;
	# Find the RealPlayer clients and parse their info
	if ($client_info =~ m/^[A-Za-z0-9]+_/) {
		@client_info_elements = split(/_/, $client_info);
		($platform, $os_version, $client_version, $type, $distribution, $language, $cpu, $embedded) = @client_info_elements;
		} elsif ($client_info =~ m/^Q/) {	# Look for QuickTime entries
		# Get the player type, the player version, the OS and the OS version
		$client_info =~ m/^(Q\w*)\s\(qtver=(\d.+\d)\;os=([A-Za-z].+)\)/;
		($type, $client_version, $os_version) = ($1, $2, $3);
		}
	my $query = qq{ INSERT INTO client VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, $access_id ) };
	$sth = $dbh->prepare($query);
	$sth->bind_param(1, $client_info);
	$sth->bind_param(2, $platform);
	$sth->bind_param(3, $os_version);
	$sth->bind_param(4, $client_version);
	$sth->bind_param(5, $type);
	$sth->bind_param(6, $distribution);
	$sth->bind_param(7, $language);
	$sth->bind_param(8, $cpu);
	$sth->bind_param(9, $embedded);
	$sth->bind_param(10, $client_GUID);
	$sth->execute;
	$sth->finish;

} # end sub parseClientInfo

# Loops through the stats mask info and inserts it
sub parseStatsMask {
	my $file_id = pop(@_);
	my $access_id = pop(@_);
	my @bracket_matches = @_;
	my $size = ($#bracket_matches + 1);	# Find the number of bracket_matches
	my @stats_masks = {};
	my $counter = 0;
	my $i;
	# Start with the position of Stats1 (a constant) and loop through the masks recorded
	for ($i = 3; $i < $size; $i++) {
		$stats_masks[$counter] = $bracket_matches[$i];
		$stats_masks[$counter] =~ m/(Stat[1-3]:)/ || /(UNKNOWN)/;	# Get the stats style
		$stats_style = $1;
		if (($stats_style ne "GET") && ($stats_style ne "UNKNOWN")){	# The "GET" comes from the main loop above
			if ($stats_style eq "Stat1:") {
			# Get the statistical information
			$stats_masks[$counter] =~ /(Stat[1-3]:)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)((\s+\w.+)||(\s+))/;
			my @stats = ("$1","$2","$3","$4","$5","$6","$7");
			foreach $i (@stats) { $i =~ s/ //g; }    # Get rid of the leading spaces
			my ($mask, $packets_received, $out_of_order, $missing, $early, $late, $audio_format) = (@stats);
			if ($audio_format eq "") { $audio_format = "UNKNOWN" }
			my $query = qq{ INSERT INTO stats_mask1 VALUES ( NULL, ?, ?, ?, ?, ?, ?, $access_id, $file_id ) };
			$sth = $dbh->prepare($query);
			$sth->bind_param(1, $packets_received);
			$sth->bind_param(2, $out_of_order);
			$sth->bind_param(3, $missing);
			$sth->bind_param(4, $early);
			$sth->bind_param(5, $late);
			$sth->bind_param(6, $audio_format);
			$sth->execute;
			$sth->finish;
			} elsif ( $stats_style eq "Stat2:") {
			# Get the statistical information
			$stats_masks[$counter] =~ /(Stat[1-3]:)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+)(\s+\d+[.]?\d*)(\s+\d+)(\s+\d+)((\s+\w.+)||(\s+))/;
			my @stats = ("$1","$2","$3","$4","$5","$6","$7","$8","$9","$10","$11","$12","$13");
			foreach $i (@stats) { $i =~ s/ //g; }    # Get rid of the leading spaces
			my ($mask, $bandwidth, $available, $highest, $lowest, $average, $requested, $received, $late, $rebuffering, $transport, $startup, $audio_format) = (@stats);
			if ($audio_format eq "") { $audio_format = "UNKNOWN" }
			my $query = qq{ INSERT INTO stats_mask2 VALUES ( NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, $access_id, $file_id ) };
			$sth = $dbh->prepare($query);
			$sth->bind_param(1, $bandwidth);
			$sth->bind_param(2, $available);
			$sth->bind_param(3, $highest);
			$sth->bind_param(4, $lowest);
			$sth->bind_param(5, $average);
			$sth->bind_param(6, $requested);
			$sth->bind_param(7, $received);
			$sth->bind_param(8, $late);
			$sth->bind_param(9, $rebuffering);
			$sth->bind_param(10, $transport);
			$sth->bind_param(11, $startup);
			$sth->bind_param(12, $audio_format);
			$sth->execute;
			$sth->finish;
			} elsif ( $stats_style eq "Stat3:") {
			my $query = qq{ INSERT INTO stats_mask3 VALUES ( NULL, ?, $access_id, $file_id ) };
			$sth = $dbh->prepare($query);
			$sth->bind_param(1, $stats_masks[$counter]);
			$sth->execute;
			$sth->finish;
			}		
		}
		$counter++;
	}
} # end sub parseStatsMask
