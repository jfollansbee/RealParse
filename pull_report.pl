#! /usr/bin/perl -w

# Creates reports for emailing to hosting customers 

use diagnostics;
use Time::localtime;
use DBI;

# Set the database connection variables
DELETED

# Get today's date
$today = localtime;
($year, $month, $day) = ($today->year+1900, $today->mon+1, $today->mday);
$today = ($year . "/" . $month . "/" . $day);

print "Today is $today.\n";

# Connect to the database
$dbh = DBI->connect("DBI:mysql:$database:$host",$username,$password) || die("Could not connect: $DBI::errstr");

my ($query, $sth);

# Get the customer info from the contacts database
my @customers = &customerInfo;

$sth = $dbh->prepare("USE streamlog"); $sth->execute; $sth->finish;

# Lock the streamlog tables
$query = qq{ LOCK TABLES access WRITE, file WRITE, client WRITE, stats_mask1 WRITE, stats_mask2 WRITE, stats_mask3 WRITE, network WRITE, components WRITE, project WRITE, project_file WRITE };
$sth = $dbh->prepare($query); $sth->execute; $sth->finish;


for ($i = 0; $i < ($#customers + 1); $i++) {
	# Using contact/customer id, get the project information.
	my @customer = @{$customers[$i]};
	my $query = qq{ SELECT * FROM project WHERE (customer_id = '$customer[0]') };
	my $sth = $dbh->prepare($query);
	$sth->execute;
	print "Customer: ", join("\t", @customer), "\n";
	# Using the project id from project, get the file information.
	while (my $project = $sth->fetchrow_arrayref) {
		my $query = qq{ SELECT * FROM project_file WHERE (project_id = '@$project[0]') };
		my $sth = $dbh->prepare($query);
		$sth->execute;
		print "Project: ", join("\t", @$project), "\n";
		# Run the reports with the file name.
		while (my $project_file = $sth->fetchrow_arrayref) {
			my $filecount = &countFiles(@$project_file[1]);
			my $cliplength = &lengthFile(@$project_file[1]);
			my $avgview = &avgviewTime(@$project_file[1]);
			my $longview = &longviewTime(@$project_file[1]);
			print "File: @$project_file[1]\n";
			print "\tCount:\t$filecount\n";
			print "\tClip Length:\t$cliplength\n";
			print "\tAvg View:\t$avgview\n";
			print "\tLongest View:\t$longview\n";
#			print "File: ", join("\t", @$project_file), "\n";
			}
		$sth->finish;
		}
	$sth->finish;
	print "\n\n";
	}

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

# Gets the customer information from the contacts database
sub customerInfo {
my $sth = $dbh->prepare("USE contacts"); $sth->execute; $sth->finish;
my $query = qq{ SELECT id, first_name, last_name, company_name, email1 FROM main WHERE (hosting = 'Yes') };
$sth = $dbh->prepare($query);
$sth->execute;
my @customers;
while (my $customer_info = $sth->fetchrow_arrayref) {
	push(@customers, [@$customer_info]);
}
$sth->finish;
return @customers;
} # end sub customerInfo

# Count the instances of the file name.
sub countFiles {
my $project_file = shift(@_);
# print "PROJECT FILE: $project_file\n";
my $query = qq{ SELECT count(file.name) AS count FROM file, access WHERE (file.name LIKE '$project_file') && ((access.client_ip_address NOT LIKE '192.168.%') && (access.access_id = file.access_id)) };	# Exclude CI internal IP addresses.
my $sth = $dbh->prepare($query);
$sth->execute;
my $filecount = $sth->fetchrow_hashref;
$sth->finish;
my $count = $filecount->{ count };
return $count;
} # end sub countFiles

# Find the file length.
sub lengthFile {
my $project_file = shift(@_);
my $length_na = "Unknown";
my $timestring;
if ((($project_file =~ m/\.wmv/) || ($project_file =~ m/\.wma/)) || ($project_file =~ m/\.mov/)) {
	return $length_na;
	} else {
	my $query = qq{ SELECT MAX(file_time) AS file_time FROM file, access WHERE (file.name LIKE '$project_file') && (file.file_time != 0) && ((access.client_ip_address NOT LIKE '192.168.%') && (access.access_id = file.access_id)) };	# Exclude CI internal IP addresses.
	my $sth = $dbh->prepare($query);
	$sth->execute;
	my $length = $sth->fetchrow_hashref;
	$sth->finish;
	my $secs = $length->{ file_time };
	if (defined($secs)) {
		$length = &formatTime($secs);
		return $length;
		} else {
		return $length_na;
		}
	}
} # end sub lengthFile 

# Calculate the average playback time. NOTE: Playback times longer than file times are excluded.
sub avgviewTime {
my $project_file = shift(@_);
my $avgview_na = "N\\A";
my $timestring;
if ((($project_file =~ m/\.wmv/) || ($project_file =~ m/\.wma/)) || ($project_file =~ m/\.mov/)) {
	return $avgview_na;
	} else {
	my $query = qq{ SELECT ROUND(AVG(file.sent_time)) AS sent_time FROM file, access WHERE (file.name LIKE '$project_file') && (file.sent_time != 0) && (file.sent_time <= file.file_time) && ((access.client_ip_address NOT LIKE '192.168.%') && (access.access_id = file.access_id)) };	# Exclude CI internal IP addresses.
	my $sth = $dbh->prepare($query);
	$sth->execute;
	my $avgview = $sth->fetchrow_hashref;
	$sth->finish;
	my $secs = $avgview->{ sent_time };
	if (defined($secs)) {
		$avgview = &formatTime($secs);
		return $avgview;
		} else {
		return $avgview_na;
		}
	}
} # end sub avgviewTime

# Find the longest playback instances, excluding times longer than the playback time.
sub longviewTime {
my $project_file = shift(@_);
my $longview_na = "N\\A";
my $timestring;
if ((($project_file =~ m/\.wmv/) || ($project_file =~ m/\.wma/)) || ($project_file =~ m/\.mov/)) {
	return $longview_na;
	} else {
	my $query = qq{ SELECT MAX(file.sent_time) AS sent_time FROM file, access WHERE (file.name LIKE '$project_file') && (file.sent_time != 0) && (file.sent_time <= file.file_time) && ((access.client_ip_address NOT LIKE '192.168.%') && (access.access_id = file.access_id)) };	# Exclude CI internal IP addresses.
	my $sth = $dbh->prepare($query);
	$sth->execute;
	my $longview = $sth->fetchrow_hashref;
	$sth->finish;
	my $secs = $longview->{ sent_time };
	if (defined($secs)) {
		$longview = &formatTime($secs);
		return $longview;
		} else {
		return $longview_na;
		}
	}
} # end sub longviewTime

# Format the time values in human readable form.
sub formatTime {
my $num_secs = shift(@_);
if ($num_secs >= 3600) {
	my $timestring = sprintf("%d:%02d:%02d", $num_secs/3600, $num_secs%3600/60, $num_secs%60);
	return $timestring;
	} else {
	$timestring = sprintf("%d:%02d", $num_secs/60, $num_secs%60);
	return $timestring;
	}
} # end sub formatTime
