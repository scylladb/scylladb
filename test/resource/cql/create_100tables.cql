// https://github.com/scylladb/scylla/issues/2923

CREATE KEYSPACE IF NOT EXISTS veraminetest WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };

USE veraminetest;

CREATE TABLE binary_global (
    md5 text,
    uploadtimestamp timeuuid,
    sha1 text,
    sha256 text,
    size int,
    imagesize int,
    filetype text,
    humanfiletype text,
    fileversion text,
    companyname text,
    filedescription text,
    internalname text,
    legalcopyright text,
    originalfilename text,
    productname text,
    produceversion text,
    signed boolean,
    certresult text,
    certcomment text,
    certsigner text,
    uploadsystemguid uuid,
    uploadcustomerguid uuid,
    uploadbasename text,
    lastuploadedoffset int,
    flags int,
    uploadpath text,
    certificatehash text,
    certificatesubject text,
    certificateissuer text,
    certificatecompanyname text,
    vtlastcheckdate timestamp,
    vtpermalink text,
    vtscandate timestamp,
    vtpositives int,
    vttotal int,
    vtscans map<text,text>,
    osirisjson blob,
    yarascandate timestamp,
    yarahits set<text>,
    type text,
    arch text,
    petype text,
    subsystem text,
    imagebase bigint,
    sizeofimage bigint,
    entrypointrva bigint,
    entrypointrawoffs bigint,
    sectioncount int,
    sections map<text,text>,
    exports map<int,text>,
    importslist list<text>,
    analyzed boolean,
    reprocess boolean,
    PRIMARY KEY (md5, uploadtimestamp)) WITH CLUSTERING ORDER BY (uploadtimestamp DESC);

CREATE TYPE hash_and_basename (
    md5 text,
    basename text
);

CREATE TABLE binaries_by_companyname (
    customerguid uuid,
    companyname text,
    certificatesubject text,
    certificatehash text,
    imagehash text,
    basenames set<text>,
    certificateissuer text,
    firstseen timestamp,
    downloaded boolean,
    policyenabled boolean,
    policyblockexecution boolean,
    policyblocknetwork boolean,
    policyblockreason text,
    category text,
    PRIMARY KEY (customerguid, companyname, certificatesubject, certificatehash, imagehash))
        WITH CLUSTERING ORDER BY (companyname ASC, certificatesubject ASC, certificatehash ASC, imagehash ASC);

CREATE TABLE trusted_certificate_companynames (
	customerguid uuid,
	certificatecompanyname text,
	PRIMARY KEY (customerguid,certificatecompanyname)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

CREATE TABLE certificate_counters (
    customerguid uuid,
    companyname text,
    certificatecount counter,
    binarycount counter,
    PRIMARY KEY (customerguid, companyname))
    WITH CLUSTERING ORDER BY (companyname ASC);

CREATE TABLE fnvhashes (
    key int,
    hash bigint,
    imagepath text,
    usersid text,
    userstring text,
    PRIMARY KEY (key, hash)) WITH CLUSTERING ORDER BY (hash ASC)
        AND CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

CREATE TABLE trusted_certificate_subjects (
	customerguid uuid,
	certificatesubject text,
	PRIMARY KEY (customerguid,certificatesubject)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

CREATE TABLE customers (
	key int,
	customerguid uuid,
    alertemails list<text>,
    alertthreshold int,
	customerid int,
	name text,
    initialemail text,
	contractstart timestamp,
	contractend timestamp,
    yarasigdate timestamp,
	idletimeoutseconds int,
	defaultcookielengthseconds int,
	PRIMARY KEY (key, customerguid)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

CREATE TABLE apikeys (
    key int,
    apikey text,
    customerguid uuid,
    PRIMARY KEY (key, apikey)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

//////////////////////////// HOST STUFF
///////////////////////////////////////
// show all hosts in the system. This table will only have ONE partition key.
// since all it stores is host information and is queried often (hosts.html), we
// will tell Cassandra to put it in memory.

CREATE TYPE disk (
    diskname text,
    disktype int,
    totalspace int,
    freespace int
);

CREATE TABLE hosts (
  key int,
  customerguid uuid,
  customerid int,
  systemguid uuid,
  hostid int,
  bytesrecved bigint,
  eventscount bigint,
  protocolbytecount bigint,  // phantom 1.0 protocol events byte counter
  uploadbytes bigint,   // phantom 1.0 file upload byte counter
  uploadcount int,          // phantom 1.0 number of files uploaded
  hostname text,
  ipstr set<text>,
  bitlockervolumesenabled set<text>,
  bitlockervolumesdisabled set<text>,
  diskinformation list<frozen<disk>>,
  wusummary text,
  lockconsoleonwake boolean,
  windowsfirewallstatus text,
  createdat timestamp,
  lastcontact timestamp,
  lastsessionstarttime timestamp,
  lastsessionendtime timestamp,
  lastsessiondisconnecttime timestamp,
  lastsystemuptime bigint,
  osver text,
  sensorversion int,
  sessionguid uuid,
  quarantined boolean,
  lastdiagnosticsjson text,
  lastdiagnosticstime timestamp,
  groups set<text>,
  PRIMARY KEY (key, customerguid, systemguid, hostid)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

// host->{session1, session2, ... }
// show all sessions from a host (see also session_info table)
CREATE TABLE host_sessions (
    systemguid uuid, //BeginSession
    customerguid uuid, // Identification
    hostname text,
    hostid int,
    sessionendtime timestamp, //EndSession
    sessiondisconnecttime timestamp,
    sessionguid uuid, //BeginSession
    sessionstarttime timestamp, //BeginSession
    timezonebias int, //BeginSession
    sessionflags int, //BeginSession
    sensorversion int, //BeginSession
    protocolbytecount bigint,  // phantom 1.0 protocol events byte counter
    uploadbytes bigint,   // phantom 1.0 file upload byte counter
    uploadcount int,          // phantom 1.0 number of files uploaded
    externaladdr text,
    PRIMARY KEY (systemguid, sessionstarttime, sessionguid, customerguid)) WITH CLUSTERING ORDER BY (sessionstarttime DESC);

// Enough info to look up a host by id
CREATE TABLE hostinfo (
	hostid int,
	systemguid uuid,
	customerguid uuid,
	customerid int,
	sessionguid uuid,
	hostname text,
	createdat timestamp,
    PRIMARY KEY (hostid));

CREATE TABLE group_hosts (
    customerguid uuid,
    groupname text,
    hostid int,
    hostname text,
    systemguid uuid,
    PRIMARY KEY (customerguid, groupname, hostid));

CREATE TABLE indicators (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	imagebasename text,
	imagehash text,
	processbasename text,
	hostname text,
	eventtime timestamp,
	type text,
	confidence int,
	processguid uuid,
	otherprocessguid uuid,
	pid bigint,
	otherpid bigint,
	description text,
	sourcesessionguid uuid,
	sourcetstamp timeuuid,
	certificatesubject text,
	commandlinematch text,
	userstring text,
	usersid text,
	PRIMARY KEY (customerguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE indicator_mods (
	customerguid uuid,
	indicatortype text,
	md5toignore map<text,text>, // md5s to ignore, optionally with ;userstring|hostname;userstring|hostname;
	basenamecertificatesubjecttoignore map<text,text>, // basename|certificatesubject -> ;userstring|hostname;userstring|hostname; to ignore
	basenametoignore map<text,boolean>, // basenames to ignore
	certsubjecttoignore map<text,boolean>, // certificate subjects to ignore -> true
	userstoignore map<text,boolean>, // userstring -> true
	usercommandstoignore map<text,text>, // basename -> ;commandline|userstring;commandline|userstring;commandline|userstring;
	machinestoignore map<text,boolean>, // hostname -> true
	md5stoupgrade map<text,boolean>,
	processguidstoupgrade map<uuid,boolean>,
	usercommandstoupgrade map<text,text>, // basename -> ;commandline|userstring;commandline|userstring;commandline|userstring;
	globaloff boolean,
	PRIMARY KEY (customerguid, indicatortype)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

/////////////////////// SESSION related stuff
/////////////////////////////////////////////

// all host/system info for the session
//
// NB: We could have merged session_info and host_sessions into one table but
// I did not do so because in case people decide to only look up things based on
// SessionGuid.
CREATE TABLE session_info (
    sessionguid uuid, //BeginSession
    customerguid uuid, // SystemInformation
    departmentname text,
    dnsdomainname text, // HostInformation
    dnshostname text, // HostInformation
	hostid int,
    domainsid text,  // HostInformation
    domainforestname text, // HostInformation
    domainguid uuid, // HostInformation
    domainname text, // HostInformation
    domainnetbiosname text, // HostInformation
    domainrole int, // HostInformation
    hardwarefamily text, // SystemInformation
    hardwaremanufacturer text, // SystemInformation
    hardwareproductname text, // SystemInformation
    hardwarenumberofprocessors int, // SystemInformation
    hardwareprocessor int, // SystemInformation
    hardwareprocessordescription text, // SystemInformation
    hardwareavailablememory int, // SystemInformation
    hardwaretotalmemory int, // SystemInformation
    ipstr set<text>,
    lastcommunicationtime timestamp,
	externaladdr text,
    locale text, // SystemInformation
    localeid int, // SystemInformation
    machinesid text, // SystemInformation
    macstr set<text>,
    netbiosname text, // HostInformation
    sensorversion int, //BeginSession, SystemInformation
    sessionendtime timestamp, //EndSession
    sessionstarttime timestamp, //BeginSession
    sessiondisconnecttime timestamp,
    sessionflags int, //BeginSession
    supported boolean, // SystemInformation
    systemguid uuid, //BeginSession
    pid4guid uuid,
    timezonename text, // SystemInformation
    timezonebias int, //BeginSession, SystemInformation
    timezonebiasadj int, // SystemInformation
    windowsversionmajor int, // SystemInformation
    windowsversionminor int, // SystemInformation
    windowsversionproducttype int, // SystemInformation
    windowsversionservicepack int, // SystemInformation
		PRIMARY KEY (sessionguid));


///////////////////////////////////// PROCESSES stuff
/////////////////////////////////////////////////////
// ALL processes

CREATE TYPE imageloadentry (
    basepathhash bigint,
    baseaddr bigint,
    loadtime timeuuid,
    imagemd5 blob,
    imageflags int,
    basename text
);

CREATE TABLE processes (
    sessionguid uuid,
    createtimestamp timeuuid,
    processguid uuid,
    customerguid uuid,
    appcontainer int,
    appcontainersid text,
    authenticationid int,
    basename text,
    commandline text,
    currentdirectory text,
    exitcode bigint,
    exittime timestamp,
    imagebase bigint,
    imagecertificatehash text,
    imagecertificateissuer text,
    imagecertificatesubject text,
    imageflags int,
    imagehash text,
    imageloads list<frozen<imageloadentry>>,
    imagepath text,
    imagesize bigint,
    parentappcontainer int,
    parentappcontainersid text,
    parentauthenticationid int,
    parentbasename text,
    parentflags int,
    parentnotindb boolean,
    parentpid bigint,
    parentprocessguid uuid,
    parenttokenenabledprivilege int,
    parenttokenflags int,
    parenttokenintegrity int,
    parenttokenprivilege int,
    parentusersid text,
    pid bigint,
    processflag_64bit boolean,
    processflag_aslr boolean,
    processflag_dep boolean,
    processflag_newprocess boolean,
    processflag_permdep boolean,
    processflags int,
    protopid bigint,
    systemguid uuid,
    tokenenabledprivilege int,
    tokenflag_isadmin boolean,
    tokenflag_isanonymous boolean,
    tokenflag_isappcontainer boolean,
    tokenflag_isauthenticated boolean,
    tokenflag_isconsolelogon boolean,
    tokenflag_iselevated boolean,
    tokenflag_isinteractive boolean,
    tokenflag_islocal boolean,
    tokenflag_islocalservice boolean,
    tokenflag_isnetwork boolean,
    tokenflag_isnetworkservice boolean,
    tokenflag_isremotelogon boolean,
    tokenflag_isrestricted boolean,
    tokenflag_isservice boolean,
    tokenflag_issystem boolean,
    tokenflags int,
    tokenintegrity int,
    tokenprivilege int,
    tstamp timeuuid,
    tstampcreate timestamp,
    tstampterminate timestamp,
    usersid text,
    userstring text,
    PRIMARY KEY (sessionguid, createtimestamp, processguid)) WITH CLUSTERING ORDER BY (createtimestamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400;

// Enough info to look up a process
CREATE TABLE sessionprocessinfo (
    processguid uuid, // ProcessCreate
    sessionguid uuid,
    customerguid uuid,
    pid bigint, // ProcessCreate
    protopid bigint,
    parentpid bigint, // ProcessCreate
    createtimestamp uuid, // ProcessCreate
    processflags int, // ProcessCreate
    basename text, // ProcessCreate
    imagehash text, // ProcessCreate
    commandline text, // ProcessCreate
    PRIMARY KEY (processguid))
        WITH compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400;

// ALL process opens
CREATE TABLE processopens (
	targetbasename text, // from ProcessOpen TargetPid
	tstamp uuid, // time.Now()
	targetprocessguid uuid, // from ProcessOpen TargetPid
	tstampraw timestamp, // ProcessOpen
	processguid uuid, // from ProcessOpen Pid
	basename text, // from ProcessOpen Pid
	pid bigint, // ProcessOpen
	imagehash text,
	targetimagehash text,
	targetpid bigint, // ProcessOpen
	desiredaccess int, // ProcessOpen
	grantedaccess int, // ProcessOpen
	inkernel boolean, // ProcessOpen
	sessionguid uuid,
	systemguid uuid,
	hostname text,
	binaryflags int, // From binary_global
	certissuer text, // From binary_global
	certsubject text, // From binary_global
	PRIMARY KEY (targetbasename, tstamp, targetprocessguid)) WITH CLUSTERING ORDER BY (tstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400
        AND gc_grace_seconds = 864000;

CREATE TABLE binaryloads (
    customerid int,
    imagehash blob,
    processstarttime timeuuid,
    basename text,
    basepathhash bigint,
    userhash bigint,
    hostid int,
    hostname text,
    pid int,
    processbasename text,
    processguid uuid,
    PRIMARY KEY ((customerid, imagehash), processstarttime)) WITH CLUSTERING ORDER BY (processstarttime DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 7776000
        AND gc_grace_seconds = 864000;

CREATE TABLE pid4imageloads (
	customerguid uuid,
	systemguid uuid,
	hashesloaded map<text,boolean>,
	PRIMARY KEY ((customerguid, systemguid))) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

CREATE TABLE suspiciouscommands (
	customerguid uuid,
	systemguid uuid,
	basename_and_commandline map<text,text>,
	PRIMARY KEY ((customerguid, systemguid)))  WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};

// thread injection
CREATE TABLE thread_injection (
    sessionguid uuid, // ThreadCreate
    tstamp timeuuid, // ThreadCreate
    tstampraw timestamp, // ThreadCreate
    hostname text,
    pid bigint, // ThreadCreate
    processguid uuid, // ThreadCreate
    sourcepid bigint, // ThreadCreate
    sourceprocessguid uuid, // ThreadCreate
    startaddress bigint, // ThreadCreate
    systemguid uuid, // ThreadCreate
    tid int, // ThreadCreate
	sourcebasename text,
	sourceimageflags int,
	sourcecertificateissuer text,
	sourcecertificatesubject text,
	sourcecertificatehash text,
	sourceimagehash text,
	targetbasename text,
	targetimageflags int,
	targetcertificateissuer text,
	targetcertificatesubject text,
	targetcertificatehash text,
	targetimagehash text,
	modulehash text,
	moduleimageflags int,
	modulebasename text,
	modulecertificateissuer text,
	modulecertificatesubject text,
	modulecertificatehash text,
	moduleoffset bigint,
	moduleexport text,
	modulenearestexport text,
    commandline text,
    PRIMARY KEY (sessionguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400
        AND gc_grace_seconds = 864000;

// token change
CREATE TABLE tokenchange (
    sessionguid uuid, // TokenChange
    tstamp timeuuid, // TokenChange
    tstampraw timestamp, // TokenChange
    pid bigint, // TokenChange
    processguid uuid, // TokenChange
    systemguid uuid, // TokenChange
	hostname text,
    newappcontainersid text, // TokenChange
    newappcontainer int, // TokenChange
    newauthenticationid int, // TokenChange
    newtokenflags int, // TokenChange
    newtokenintegrity int, // TokenChange
    newtokenprivilege int, // TokenChange
    newtokenenabledprivilege int, // TokenChange
    newusersid text, // TokenChange
    oldappcontainersid text, // TokenChange
    oldappcontainer int, // TokenChange
    oldauthenticationid int, // TokenChange
    oldtokenflags int, // TokenChange
    oldtokenintegrity int, // TokenChange
    oldtokenprivilege int, // TokenChange
    oldtokenenabledprivilege int, // TokenChange
    oldusersid text, // TokenChange
	parentprocessguid uuid,
    parentappcontainersid text, // ProcessCreate
    parentappcontainer int, // ProcessCreate
    parentauthenticationid int, // ProcessCreate
    parenttokenflags int, // ProcessCreate
    parenttokenintegrity int, // ProcessCreate
    parenttokenprivilege int, // ProcessCreate
    parenttokenenabledprivilege int, // ProcessCreate
    parentusersid text, // ProcessCreate
    PRIMARY KEY (sessionguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400
        AND gc_grace_seconds = 864000;


// holds file change info
CREATE TABLE filechange (
    sessionguid uuid, // FileChange
    tstamp timeuuid, // FileChange
    tstampraw timestamp, // FileChange
    filepath text, // FileChange
    hostname text,
    newchangetime timestamp, // FileChange
    newcreatetime timestamp, // FileChange
    newwritetime timestamp, // FileChange
    oldchangetime timestamp, // FileChange
    oldcreatetime timestamp, // FileChange
    oldwritetime timestamp, // FileChange
    pid bigint, // FileChange
    processguid uuid, // FileChange
    systemguid uuid, // FileChange
    PRIMARY KEY (sessionguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 86400
        AND gc_grace_seconds = 864000;

/////////////////////////////Network related stuff
///////////////////////////////////////////////////

// holds IP to session/host
CREATE TABLE ip_sessions (
    ip inet, // NetworkInformation
    tstamp timeuuid, // NetworkInformation
    adaptername text, // NetworkInformation
    dnsservers map<text,int>, // NetworkInformation
    gateways map<text,int>, // NetworkInformation
    hostname text,
    ipstr text, // NetworkInformation
    macstr text, // NetworkInformation
    iptype int, // NetworkInformation
    sessionguid uuid, // NetworkInformation
    systemguid uuid, // NetworkInformation
    PRIMARY KEY (ip, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// holds MAC to sessions/hosts
CREATE TABLE mac_sessions (
    mac text, // NetworkInformation
    tstamp timeuuid, // NetworkInformation
    adaptername text, // NetworkInformation
    dnsservers map<text,int>, // NetworkInformation
    gateways map<text,int>, // NetworkInformation
    hostname text,
    ipstrmap map<text,int>, // NetworkInformation
    macstr text,
    mactype int, // NetworkInformation
    status int, // NetworkInformation
    sessionguid uuid, // NetworkInformation
    systemguid uuid, // NetworkInformation
    PRIMARY KEY (mac, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

////////////////////////// USERS and GROUPS stuff
//////////////////////////////////////
// we can use collection type here but I was thikning what if people have more
// than 65k users or groups

// holds all users on a host
CREATE TABLE host_users (
    systemguid uuid, // UserInformation
    tstamp timeuuid, // UserInformation
    sessionguid uuid, // UserInformation
    userid int, // UserInformation
    username text, // UserInformation
    fullname text, // UserInformation
    usersid text, // UserInformation
    PRIMARY KEY (systemguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// holds all groups on a host
CREATE TABLE host_groups (
    groupname text, // GroupInformation
    tstamp timeuuid, // GroupInformation
    groupsid text, // GroupInformation
    sessionguid uuid, // GroupInformation
    systemguid uuid, // GroupInformation
    PRIMARY KEY (groupname, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// user to groups
// holds a user -> groups association
CREATE TABLE user_groups (
    username text, // GroupInformation
    groups set<text>, // GroupInformation
    hostname text,
    sessionguid uuid, // GroupInformation
    systemguid uuid, // GroupInformation
    PRIMARY KEY (username));

CREATE TABLE local_administrators (
	customerguid uuid,
	systemguid uuid,
	usersids set<text>,
	PRIMARY KEY ((customerguid, systemguid))) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };

CREATE TABLE administrators_by_sid (
    customerguid uuid,
    usersid text,
    systemguid uuid,
    lastreported timestamp,
    PRIMARY KEY (customerguid, usersid, systemguid));

CREATE TABLE userinfo_by_sid (
	customerguid uuid,
	usersid text,
    systemguid uuid,
    hostid int,
	userstring text,
	userdomain text,
	username text,
	fullname text,
    userflags int,
    userflag_local boolean,
    userflag_admin boolean,
    userflag_user boolean,
	PRIMARY KEY ((customerguid, usersid))) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };


// user_events_by_customer used to fetch one row per session
CREATE TABLE user_events_by_customer (
    customerguid uuid,
    systemguid uuid,
    sessionid bigint,
    sessionstart timestamp,
    fullname text,
    hostid int,
    hostname text,
    latestflags int,
    remoteclient text,
    remotehostname text,
    remoteipstr text,
    sessionchange timestamp,
    sessionend timestamp,
    sessionflag_active boolean,
    sessionflag_console boolean,
    sessionflag_locked boolean,
    sessionflag_offline boolean,
    sessionflag_remote boolean,
    sessionflag_shadowed boolean,
    sessionflag_userlogged boolean,
    sessionflag_valid boolean,
    sessionguid uuid,
    userid int,
    username text,
    usersid text,
    windowstation text,
    PRIMARY KEY (customerguid,sessionstart,systemguid)) WITH CLUSTERING ORDER BY (sessionstart ASC, systemguid ASC);

// User_events_by_user is inserted into by kex.go.  It is not updated with the current status.  Just a new row with each logon, logoff, or change
//  This table is used to display "User Activity" on cportal trackuser.html page
CREATE TABLE user_events_by_user (
	customerguid uuid,
	usersid text,
	sessionstart timestamp,
	sessionid bigint,
	sessionguid uuid,
	eventtype int, // Logon, Logoff, or Change
	eventtime timestamp,
	flags int,
	systemguid uuid,
	username text static,
	fullname text static,
	userid int static,
    idlesince timestamp,
	windowstation text,
	remoteclient text,
	remotehostname text,
	remoteipstr text,
	hostid int,
	hostname text,
	sessionflag_valid boolean,
	sessionflag_userlogged boolean,
	sessionflag_active boolean,
	sessionflag_console boolean,
	sessionflag_remote boolean,
	sessionflag_locked boolean,
	sessionflag_shadowed boolean,
	sessionflag_offline boolean,
	PRIMARY KEY ((customerguid, usersid), sessionstart, sessionid, eventtype, flags)) WITH CLUSTERING ORDER BY (sessionstart DESC);

// User_events_by_host is inserted into by kex.go.  It is not updated with the current status.  Just a new row with each logon, logoff, or change
//  This table is used to display "User Activity" on cportal trackhost.html page
CREATE TABLE user_events_by_host (
	customerguid uuid,
	systemguid uuid,
	sessionstart timestamp,
	sessionid bigint,
	eventtype int, // Logon, Logoff, or Change
	flags int,
	sessionguid uuid,
	eventtime timestamp,
	usersid text,
	username text,
	fullname text,
	userid int,
	windowstation text,
	remoteclient text,
	remotehostname text,
	remoteipstr text,
	hostid int static,
	hostname text static,
	sessionflag_valid boolean,
	sessionflag_userlogged boolean,
	sessionflag_active boolean,
	sessionflag_console boolean,
	sessionflag_remote boolean,
	sessionflag_locked boolean,
	sessionflag_shadowed boolean,
	sessionflag_offline boolean,
	PRIMARY KEY ((customerguid, systemguid), sessionstart, sessionid, eventtype, flags)) WITH CLUSTERING ORDER BY (sessionstart DESC);

// User_events_by_sessionid is a lookup table to fetch session metadata needed to insert rows into other tables
//  Change and Logoff messages report only a SessionId and not the other metadata.  Kex.go caches the metadata while it is running but if Kex
//  in-memory cache does not have metadata it will look it up from this table (and then add it to the in-memory cache)

CREATE TABLE user_events_by_sessionid (
	customerguid uuid,
	systemguid uuid,
	sessionguid uuid,
	sessionid bigint,
	usersid text,
	username text,
	fullname text,
	userid int,
	sessionstart timestamp,
	windowstation text,
	remoteclient text,
	remotehostname text,
	remoteipstr text,
	PRIMARY KEY (customerguid, systemguid, sessionguid, sessionid));

CREATE TABLE user_events_primary_workstation (
	customerguid uuid,
	usersid text,
	username text,
	workstations map<uuid,int>,
	downgrades map<uuid,boolean>,
	PRIMARY KEY (customerguid, usersid)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };

CREATE TABLE user_events_primary_user (
	customerguid uuid,
	systemguid uuid,
	hostid int,
	users map<text,int>,
	exceptions map<text,boolean>,
	PRIMARY KEY (customerguid, systemguid)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };

CREATE TABLE registry_activity (
    customerguid uuid,
    systemguid uuid,
    sessionguid uuid,
    processguid uuid,
    tstamp timeuuid,
    activitytype int,
    keypath text,
    deletevalue text,
    desiredaccess int,
    grantedaccess int,
    renamedname text,
    replacenewname text,
    replaceoldname text,
    restorefilepath text,
    restoreflags int,
    savefilepath text,
    saveformat int,
    setdata blob,
    setdatatype int,
    setinformationclass int,
    setsecurityinformation int,
    setsize int,
    setvalue text,
    status bigint,
    PRIMARY KEY ((customerguid, systemguid, sessionguid), processguid, tstamp))
    WITH CLUSTERING ORDER BY (processguid DESC, tstamp DESC)
    AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
    AND default_time_to_live = 86400
    AND gc_grace_seconds = 864000;

CREATE TABLE network_connections3 (
    customerguid uuid,
    hostid int,
    starttstamp timeuuid,
    processguid uuid,
    connectiontype int,
    flowid bigint,
    bindid bigint,
    processbasename text,
    pid int,
    unsignedprocess boolean,
    direction int,
    localaddress text,
    localport int,
    protocol int,
    remoteaddress text,
    remoteport int,
    remotednsname text,
    active boolean,
    endtime timestamp,
    duration bigint,
    datalengthin bigint,
    datalengthout bigint,
    PRIMARY KEY ((customerguid, hostid), starttstamp))
    WITH CLUSTERING ORDER BY (starttstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 864000
        AND gc_grace_seconds = 30;



CREATE TABLE network_connections2 (
    customerguid uuid,
    systemguid uuid,
    sessionguid uuid,
    protopid bigint,
    tstamp timeuuid,
    processguid uuid,
    connectiontype int,
    flowid bigint,
    bindid bigint,
    processbasename text,
    unsignedprocess boolean,
    direction int,
    localaddress text,
    localport int,
    protocol int,
    remoteaddress text,
    remoteport int,
    remotednsname text,
    active boolean,
    starttime timestamp,
    endtime timestamp,
    duration bigint,
    datalengthin bigint,
    datalengthout bigint,
    hostname text,
    PRIMARY KEY (customerguid, systemguid, sessionguid, protopid, tstamp))
      WITH CLUSTERING ORDER BY (systemguid DESC, sessionguid DESC, protopid DESC, tstamp DESC)
        AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
        AND default_time_to_live = 864000
        AND gc_grace_seconds = 30;

CREATE TABLE network_connections (
	bindid bigint,
	connectiontype int,
	customerguid uuid,
	flowid bigint,
	processguid uuid,
	protopid bigint,
	systemguid uuid,
	tstamp timeuuid,
	active boolean,
	datalengthin bigint,
	datalengthout bigint,
	direction int,
	endtime timestamp,
	lastupdate timestamp,
	localaddress text,
	localport int,
	processbasename text,
	protocol int,
	remoteaddress text,
	remoteport int,
	remotednsname text,
	sessionguid uuid,
	starttime timestamp,
	duration bigint,
	PRIMARY KEY ((customerguid, systemguid, processguid, connectiontype, flowid, bindid), tstamp))
	  WITH CLUSTERING ORDER BY (tstamp DESC)
      AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
      AND default_time_to_live = 86400
      AND gc_grace_seconds = 30;

CREATE TABLE dns_name_cache (
	customerguid uuid,
	systemguid uuid,
	ipaddress text,
	tstamp timeuuid,
	dnsname text,
	PRIMARY KEY ((customerguid, systemguid, ipaddress), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE services (
	customerguid uuid,
	systemguid uuid,
	sessionguid uuid,
	processguid uuid,
	starttstamp timeuuid,
	endtstamp timeuuid,
	changetstamp timeuuid,
	serviceid bigint,
	servicename text,
	servicepath text,
	servicedll text,
	serviceflags int,
	serviceimagepath text,
	serviceimagehash text,
	serviceimagecreatetime timestamp,
	serviceimagewritetime timestamp,
	serviceimagechangetime timestamp,
	serviceimagecertificatehash text,
	serviceimagecertificatesubject text,
	serviceimagecertificatecategory text,
	serviceimageflags int,
	pid bigint,
	PRIMARY KEY ((customerguid, systemguid, sessionguid, serviceid), starttstamp)) WITH CLUSTERING ORDER BY (starttstamp DESC);

CREATE TABLE autoruns (
	customerguid uuid,
	systemguid uuid,
	createtstamp timeuuid,
	type int,
	servicename text,
	sessionguid uuid,
	processguid uuid,
	serviceid bigint,
	PRIMARY KEY ((customerguid, systemguid, type, servicename), createtstamp)) WITH CLUSTERING ORDER BY (createtstamp DESC);

CREATE TABLE smbshares (
    customerguid uuid,
    systemguid uuid,
    sharename text,
    allshareids set<text>,
    changed timeuuid,
    created timeuuid,
    deleted timeuuid,
    lastflags int,
    lastownername text,
    lastownersid text,
    lastpath text,
    lastshareid text,
    lastupdated timeuuid,
    deceptionshare boolean,
	PRIMARY KEY ((customerguid, systemguid), sharename)) WITH CLUSTERING ORDER BY (sharename ASC);

CREATE TABLE smbsessions (
    customerguid uuid,
    systemguid uuid,
    sessionid text,
    userstring text,
    userdomain text,
    username text,
    ipaddresses list<text>,
    userflags int,
    userflag_local boolean,
    userflag_pwdnotreq boolean,
    userflag_admin boolean,
    userflag_user boolean,
    userflag_guest boolean,
    sessionflags int,
    sessionflag_authenticated boolean,
    started timeuuid,
    terminated timeuuid,
    PRIMARY KEY ((customerguid, systemguid), sessionid)) WITH CLUSTERING ORDER BY (sessionid ASC);

CREATE TABLE smbconnections (
    customerguid uuid,
    systemguid uuid,
    sharename text,
    connected timeuuid,
    treeid text,
    pathname text,
    servername text,
    shareid text,
    sessionid text,
	userstring text,
	userdomain text,
	username text,
    usersid text,
    ipaddresses list<text>,
    userflags int,
    userflag_local boolean,
    userflag_pwdnotreq boolean,
    userflag_admin boolean,
    userflag_user boolean,
    userflag_guest boolean,
    sessionflags int,
    sessionflag_authenticated boolean,
    shareflags int,
	shareflag_special boolean,
	shareflag_disk boolean,
	shareflag_ipc boolean,
	shareflag_printer boolean,
    accessmask int,
	accessmask_read boolean,
	accessmask_change boolean,
	accessmask_full boolean,
    disconnected timeuuid,
    hostid int,
    hostname text,
	PRIMARY KEY ((customerguid, systemguid), sharename, connected, treeid)) WITH CLUSTERING ORDER BY (sharename ASC, connected DESC);

CREATE TABLE smbfilecreates (
    customerguid uuid,
    systemguid uuid,
    tstamp timeuuid,
    sessionid text,
    sharename text,
    smbpath text,
    userstring text,
    userdomain text,
    username text,
    usersid text,
    ipaddresses list<text>,
    userflags int,
    userflag_local boolean,
    userflag_pwdnotreq boolean,
    userflag_admin boolean,
    userflag_user boolean,
    userflag_guest boolean,
    sessionflags int,
    sessionflag_authenticated boolean,
    accessmask int,
    accessmask_read boolean,
    accessmask_change boolean,
    accessmask_full boolean,
    hostid int,
    hostname text,
    PRIMARY KEY ((customerguid, systemguid), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);


CREATE TABLE ml_smbsessions_detect (
   partitionid int,
   tstamp timeuuid,
   eventime timestamp,
   customerguid uuid,
   systemguid uuid,
   usersid text,
   userstring text,
   useripaddresses list<text>,
   smbserver text,
   smbsharename text,
   smbpathname text,
   shareflag_special boolean,
   shareflag_disk boolean,
   shareflag_ipc boolean,
   shareflag_printer boolean,
   accessmask_read boolean,
   accessmask_change boolean,
   accessmask_full boolean,
   userflags_local boolean,
   userflags_admin boolean,
   userflags_user boolean,
   userflags_guest boolean,
   PRIMARY KEY (partitionid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC)
              AND compaction = { 'class' : 'TimeWindowCompactionStrategy' }
              AND default_time_to_live = 691200
              AND gc_grace_seconds = 0;

// Need this for the portal
CREATE TABLE connection_count_by_systemguid (
	customerguid uuid,
	systemguid uuid,
	connectiontype int,
	connectioncount counter,
	PRIMARY KEY ((customerguid, systemguid), connectiontype));

CREATE TABLE bitlocker_setting (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	volume text,
	diskletter text,
	encryptionenabled boolean,
	PRIMARY KEY ((customerguid, systemguid), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE windowsupdate_setting (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	configuration int,
	lastsearchdate timestamp,
	lastinstalldate timestamp,
	required boolean,
	updatestoinstall int,
	PRIMARY KEY ((customerguid, systemguid), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE power_setting (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	schemename text,
	active boolean,
	lockconsoleonwake boolean,
	standbytimeout int,
	hibernatetimeout int,
	lockconsoleonwakeonbattery boolean,
	standbytimeoutonbattery int,
	hibernatetimeoutonbattery int,
	PRIMARY KEY ((customerguid, systemguid), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE windowsfirewall_setting (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	domainprofileenabled boolean,
	domainblockinbound boolean,
	domainblockoutbound boolean,
	privateprofileenabled boolean,
	privateblockinbound boolean,
	privateblockoutbound boolean,
	publicprofileenabled boolean,
	publicblockinbound boolean,
	publicblockoutbound boolean,
	PRIMARY KEY ((customerguid, systemguid), tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE notifyevts (
    customerguid uuid,
    notifymask int, // indicate what type of events users want to trigger on
                    // 0x00000001 = image MD5
                    // 0x00000002 = image name
                    // 0x00000004 = non-ASLR
                    // 0x00000008 = remote ip
                    // 0x00000010 = remote ip + port
                    // 0x00000020 = logon username
                    // 0x00000040 = logon computername
                    // 0x00000080 = new connection from computername
                    // 0x00000100 = exact command line match
    imgmd5 map<text, boolean>,
    imgname map<text, boolean>,
    commandline map<text, boolean>,
    ip map<text,boolean>,
    ipport map<text, int>,
    logonuser map<text, boolean>,
    logoncomputer map<text, boolean>,
    sessioncomputer map<text, boolean>,
    sendtoemails set<text>,
    PRIMARY KEY (customerguid)) WITH CACHING = { 'keys':'ALL', 'rows_per_partition':'ALL'};

CREATE TABLE notifyevtsresult (
    customerguid uuid,
    evttime timeuuid,
    notifymask int, // indicate what type of events users want to trigger on
                    // 0x00000001 = image MD5
                    // 0x00000002 = image name
                    // 0x00000004 = non-ASLR
                    // 0x00000008 = remote ip
                    // 0x00000010 = remote ip + port
                    // 0x00000020 = logon username
                    // 0x00000040 = logon computername
                    // 0x00000080 = new connection from computername
                    // 0x00000100 = exact command line match
    systemguid uuid,
	  processguid uuid,
    imgmd5 text,
    imgname text,
    commandline text,
	  ipstr text,
	  port int,
    logonuser text,
	  usersid text,
    logoncomputer text,
	  sessioncomputer text,
	  hostid int,
    PRIMARY KEY (customerguid, evttime, notifymask)) WITH CLUSTERING ORDER BY (evttime DESC);


/////////////////////////////Configuration related stuff
////////////////////////////////////////////////////////

// default configuration for any host belonging to customerguid
//  will use this if no client_configuration specific to host
CREATE TABLE customer_configurations (
	customerguid uuid,
	updatedat timestamp,
	eventurl text,
	controlurl text,
	proxyurl text,
	reconnectinterval int,
	updateinterval int,
	sensorversion int,
	sensorfilesize int,
	sensorfilepath text,
	sensorbasefilepath text,
	sensorcreatedat timestamp,
	configid int,
    policyid int,
	policyblob blob,
    deceptionid int,
    deceptionblob blob,
    deceptionpluginid int,
    installsensormd5 text,
    updatesensormd5 text,
	PRIMARY KEY (customerguid));

// configuration for specific host
CREATE TABLE client_configurations (
	customerguid uuid,
	systemguid uuid,
	hostid int,
	updatedat timestamp,
	eventurl text,
	controlurl text,
	proxyurl text,
	reconnectinterval int,
	updateinterval int,
	sensorversion int,
	sensorfilesize int,
	sensorfilepath text,
	sensorcreatedat timestamp,
	nextaction int,
	configid int,
    policyid int,
    policyblob blob,
    deceptionid int,
    deceptionblob blob,
    deceptionpluginid int,
	PRIMARY KEY (customerguid, systemguid));

// queued actions for all hosts of a customer
CREATE TABLE customer_actions (
	customerguid uuid,
	sendafter timeuuid,
    senttosystemguids set<uuid>,
	updatedat timestamp,
	action int,
	PRIMARY KEY (customerguid, sendafter)) WITH CLUSTERING ORDER BY (sendafter ASC);

// queued actions for specific host
CREATE TABLE client_actions (
    customerguid uuid,
    systemguid uuid,
    sendafter timeuuid,
    hostid int,
    hostname text,
    createdat timestamp,
    action int,
    actionex int,
    pids set<bigint>,
    processguid uuid,
    processbasename text,
    actionid bigint,
    actionflags int,
    accountname text,
    accountdomain text,
    accountserver text,
    accountsid text,
    groupname text,
    groupactionid bigint,
    yaraexpression text,
    yaraflags int,
    yaravmlimit bigint,
    yaramatchlimit int,
    yaramatchlimitprc int,
    yaramaxprocnum int,
    yaratimeout int,
    alreadysent boolean, // working around cassandra issue
    PRIMARY KEY (customerguid, systemguid, sendafter)) WITH CLUSTERING ORDER BY (systemguid ASC, sendafter ASC)
    AND CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL'};


CREATE TABLE action_status (
	customerguid uuid,
	systemguid uuid,
    actionid bigint,
    actionextended int,
    actionex int,
    action int,
    pids set<bigint>,
    processguid uuid,
    processbasename text,
    hostid int,
    hostname text,
    accountname text,
    accountdomain text,
    accountserver text,
    accountsid text,
    imagehash text,
    lastupdate timestamp,
    status bigint,
    yaramatches int,
    yararemaining int,
    uploadoffset bigint,
    uploadsize bigint,

    // need to add all these because an action might be rejected and need to be resubmitted as client_action
    yaraexpression text,
    yaraflags int,
    yaravmlimit bigint,
    yaramatchlimit int,
    yaramatchlimitprc int,
    yaramaxprocnum int,
    yaratimeout int,
    actionflags int,

    PRIMARY KEY (customerguid, systemguid, actionid)) WITH CLUSTERING ORDER BY (systemguid ASC, actionid DESC);

CREATE TABLE deceptionpolicies (
    customerguid uuid,
    deceptionid int,
    deceptionpb blob,
    deceptionjson blob,
    created timestamp,
    createdby text,
    description text,
    currentlyactive boolean,
    PRIMARY KEY (customerguid, deceptionid)) WITH CLUSTERING ORDER BY (deceptionid desc);

CREATE TABLE process_memory_dumps (
    customerguid uuid,
    processguid uuid,
	tstamp timeuuid,
    filepath text,
    friendlyname text,
    filesize bigint,
    hostid int,
    hostname text,
    processbasename text,
    pid int,
    actionid bigint,
    PRIMARY KEY (customerguid, processguid, tstamp)) WITH CLUSTERING ORDER BY (processguid ASC, tstamp DESC);

CREATE TABLE system_memory_dumps (
    customerguid uuid,
    systemguid uuid,
	tstamp timeuuid,
    filepath text,
    friendlyname text,
    filesize bigint,
    hostid int,
    hostname text,
    actionid bigint,
    PRIMARY KEY (customerguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// storing the controlflags from every ControlRequest
CREATE TABLE client_status (
	systemguid uuid,
	tstamp timeuuid,
	customerguid uuid,
    sessionguid uuid,
    hostname text,
    hostid int,
	controlflags int,
    remoteaddr text,
	PRIMARY KEY (systemguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE clients_in_fallback_mode (
	customerguid uuid,
	systemguid uuid,
	hostid int,
	hostname text,
	tstamp timeuuid,
	PRIMARY KEY (customerguid, systemguid));

// store client messages from UploadDataRequest
CREATE TABLE client_uploads (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	datatype int,
	imagehash text,
	identifier bigint,
	serverpath text,
	fullsize int,
	lastsize int,
	lastoffset int,
	clientpath text,
	clientflags int,
	clientcerthash text,
	clientcertsubject text,
	clientcertissuer text,
	PRIMARY KEY ((customerguid,systemguid,datatype),tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE client_crashes (
	customerguid uuid,
	datatype int,
	imagehash text,
	systemguid uuid,
	hostid int,
	serverpath text,
	size int,
	tstampraw timestamp,
	PRIMARY KEY (customerguid,datatype,imagehash));

// useful for deploying updates to groups
CREATE TABLE system_group (
	customerguid uuid,
	tstamp timeuuid,
	name text,
	hostids map<int,uuid>,
	updatedat timestamp,
	latestsensorversion int,
	latestsensorfilesize int,
	latestsensorfilepath text,
	latesteventurl text,
	latestcontrolurl text,
	latestproxyurl text,
	latestreconnectinterval int,
	latestupdateinterval int,
	latestconfigid int,
	latestactionsent int,
	PRIMARY KEY (customerguid, name, tstamp));

// written out at the time a configuration is pushed in a UpdateReply
CREATE TABLE client_deployment (
	customerguid uuid,
	systemguid uuid,
	tstamp timeuuid,
	eventurl text,
	controlurl text,
	proxyurl text,
	reconnectinterval int,
	updateinterval int,
	sensorversion int,
	sensorfilesize int,
	sensorfilepath text,
	previoussensorversion int,
	configid int,
	previousconfigid int,
	actionsent int,
	PRIMARY KEY (customerguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE troubled_hosts (
	key int,
	systemguid uuid,
	customerguid uuid,
	hostid int,
	miss text,
	misstime timestamp,
	PRIMARY KEY (key,systemguid));

// really only need this to get an integer-based hostid
//  portal users type in hostid, dont want them to type in guid
CREATE TABLE counters (
    key int,
	type text,
	nextid int,
	PRIMARY KEY (key,type)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };

// server configuration variables
CREATE TABLE server_configuration (
	key int,
	tstamp timeuuid,
	uploadfilepath text,
	logfilepath text,
	uploaddatafilepath text,
	clientfilepath text,
	freespaceatuploadpath bigint,
	freespaceatlogfilepath bigint,
	freespaceatuploaddatapath bigint,
	totalspaceatuploadpath bigint,
	totalspaceatlogfilepath bigint,
	totalspaceatuploaddatapath bigint,
	adminemail text,
	alertthreshold int,
	alertthresholddouble double,
	smtpserver text,
	syslogserver text,
	eventurl text,
	controlurl text,
	PRIMARY KEY (key,tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// all application errors written out to this auditlog.
//  likely will use it for other events as well in future
CREATE TABLE auditlog (
	systemguid uuid,
	tstamp timeuuid,
	customerguid uuid,
	sessionguid uuid,
	message text,
	PRIMARY KEY (systemguid,tstamp,customerguid)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE databasestats (
  key int,
	tstamp timeuuid,
	serverstarttime timestamp,
  interactions map<text,bigint>,
	PRIMARY KEY (key, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// users on the web portal
CREATE TABLE portalusers (
	name text,
	email text,
	passwordhash blob,
	admin boolean,
	customerguid uuid,
	tzlocation text,
	authy_enabled boolean,
	authy_id int,
	PRIMARY KEY (email,customerguid));

CREATE TABLE weblogs (
	tstamp timeuuid,
	page text,
	request text,
	useremail text,
	usercustomerguid uuid,
	useradmin boolean,
	PRIMARY KEY (page,tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE onetimedownloads (
	code text,
	customerguid text,
	purpose text,
	createdat timestamp,
	downloadedat timestamp,
	downloadedby text,
	active boolean,
	sensorfilesize int,
	sensorfilepath text,
	sensorversion int,
	sensorversionstring text,
	PRIMARY KEY (code));


CREATE TABLE sensorbinaries (
	customerguid uuid,
    md5 text,
    customerid int,
    uploadfilename text,
    uploadtime timestamp,
    uploadedby text,
    versionstring text,
    version int,
    typestring text,
    filesize int,
    fulljson blob,
    filepath text,
    isupdate boolean,
    PRIMARY KEY (customerguid, md5));

CREATE TABLE pluginbinaries (
	customerguid uuid,
    pluginid int,
    filepath text,
    type text,
    PRIMARY KEY (customerguid, pluginid));

// queue of files to submit to virustotal
CREATE TABLE newbinaries (
	md5 text,
	fullpath text,
	tstamp timeuuid,
	osirisjson blob,
    vtpriority int,
	PRIMARY KEY (md5,tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

// canary is a table used to store a canary message. It should only have
// one entry at all time.  The purpose is that we would send a canary
// message, check it, and delete/update it.
CREATE TABLE canary (
	id int,
	message text,
	// when the message was generated
	tstamp timestamp,
	// ctstamp is when the we _attempted_ to write to C*. usually this value is
	// generated by whoever tried to write this record to C*; in our case it
	// would be "backend". Note that these timestamps might not be sequential due
	// to the fact that the clocks may not be synchronized.  Under ideal conditions
	// ctstamp will always come after tstamp.
	// the unit is ms since UNIX epoc.
	ctstamp timestamp,
	// cwrittentime is the time the entry was written to C*. This field is here
	// so that we can track how long it took to write to C*. This number should
	// be very small (like <10ms). The unit is ms since UNIX epoch.
	// ctstamp and cwrittentime are generated by the same process so their diference
	// is actually meaningful
	cwrittentime timestamp,
	PRIMARY KEY (id));

CREATE TABLE serverbinaries (
    md5 text,
    tstamp timeuuid,
    bytes blob,
    PRIMARY KEY (md5));

CREATE TABLE sigcheckqueue (
    md5 text,
    tstamp timeuuid,
    detectionjson blob,
    PRIMARY KEY(md5,tstamp));

CREATE TABLE veraminetest.unverified_binaries (
    customerguid uuid,
    systemguid uuid,
    certificatesubject text,
    certificatehash text,
    imagehash text,
    basename text,
    certificateissuer text,
    dbimageflags int,
    imageflags int,
    reprocess boolean,
    tstamp timestamp,
    PRIMARY KEY (customerguid, systemguid, certificatesubject, certificatehash, imagehash))
        WITH CLUSTERING ORDER BY (systemguid ASC, certificatesubject ASC, certificatehash ASC, imagehash ASC);

////////////////////////////////////////////////////////////

CREATE TABLE thread_injection_safelist (
	customerguid uuid,
	tstamp timeuuid,
	systemguid uuid,
	customerid int,
	hostid int,
	commandline text,
	commandlinelike text,
	sourcebasename text,
	sourcebasenamelike text,
	sourcemicrosoftsigned boolean,
	sourcecertificateissuer text,
	sourcecertificatesubject text,
	sourcecertificatehash text,
	moduleexport text,
	modulenearestexport text,
	modulebasename text,
	modulebasenamelike text,
	modulemicrosoftsigned boolean,
	modulecertificateissuer text,
	modulecertificatesubject text,
	modulecertificatehash text,
	targetbasename text,
	targetbasenamelike text,
	targetmicrosoftsigned boolean,
	targetcertificateissuer text,
	targetcertificatesubject text,
	targetcertificatehash text,
	description text,
	PRIMARY KEY (customerguid, tstamp)) WITH CLUSTERING ORDER BY (tstamp DESC);

////////////////////////DETECTION////////////////////////////////////

CREATE TABLE detectionbuckets (
    customerguid uuid,
    bucketname text,

    bucketdetectiontype text,
    bucketbasename text,
    bucketusername text,
    bucketsystemguid uuid,
    bucketcertificatesubject text,

    risk int,
    certificatetrustedcategory text,
    firstseen timestamp,
    lastseen timestamp,

    systemguids set<uuid>,
    usersids set<text>,
    portalstate text,
    falsepositive boolean,

    label text,
    shortdescription text,
    longdescription text,

    PRIMARY KEY (customerguid, bucketname)) WITH CLUSTERING ORDER BY (bucketname ASC);

CREATE TABLE detectioncounters (
    customerguid uuid,
    bucketname text,
    detectioncount counter,
    PRIMARY KEY (customerguid, bucketname))
    WITH CLUSTERING ORDER BY (bucketname ASC);

CREATE TABLE detectioncounters_by_systemguid (
    customerguid uuid,
    systemguid uuid,
    bucketname text,
    detectioncount counter,
    PRIMARY KEY ((customerguid, systemguid), bucketname))
    WITH CLUSTERING ORDER BY (bucketname ASC);

CREATE TABLE detectioncounters_by_usersid (
    customerguid uuid,
    usersid text,
    bucketname text,
    detectioncount counter,
    PRIMARY KEY ((customerguid, usersid), bucketname))
    WITH CLUSTERING ORDER BY (bucketname ASC);

CREATE TABLE detections (
    customerguid uuid,
    bucketname text,
    eventtimeuuid timeuuid,

    detectiontype text,
    basename text,

    certificatesubject text,
    certificatehash text,
    certificateissuer text,

    containerprocess boolean,
    description text,
    plaindescription text,

    hostid int,
    hostname text,

    imagebasename text,
    imagecertsubject text,
    imageexport text,
    imageflags int,
    imagehash text,
    imageoffset int,
    imagepath text,

    otherpid int,
    otherprocessbasename text,
    otherprocesscertsubject text,
    otherprocesscommandline text,
    otherprocessguid uuid,
    otherprocessflags int,
    otherprocessimagehash text,

    pid int,
    processbasename text,
    processcommandline text,
    processguid uuid,
    processflags int,
    processimagehash text,

    sessionguid uuid,
    systemguid uuid,

    usersid text,
    userstring text,

    PRIMARY KEY (customerguid, bucketname, eventtimeuuid)) WITH CLUSTERING ORDER BY (bucketname ASC, eventtimeuuid DESC);


CREATE TABLE detections_exclusions (
    customerguid uuid,
    detectiontype text,
    disabledglobally boolean,
    groups set<text>,
    hostids set<int>,
    basenames set<text>,
    certificatesubjects set<text>,
    usersids set<text>,
    bucketnames set<text>,
    PRIMARY KEY (customerguid,detectiontype));

CREATE TABLE suspiciousbinaries (
    customerguid uuid,
    imagehash text,
    certificatehash text,
    sha256 text,
    imagesize int,
    companyname text,
    category text,
    basenames set<text>,
    downloaded boolean,
    vtpositives int,
    vttotal int,
    hostcount int,
    loadcount int,
    firstseen timestamp,
    markedsafe boolean,
    note text,
    policyblockexecution boolean,
    policyblocknetwork boolean,
    PRIMARY KEY (customerguid, imagehash));



CREATE TABLE unsigned_unscanned_detected_binaries (
    imagehash text,
    customerguid uuid,
    bucketname text,
    lastseen timestamp,
    PRIMARY KEY (imagehash, customerguid, bucketname));


CREATE TABLE spoolsv_unsigned_imageloads (
    customerguid uuid,
    imagehash text,
    firstimagebasename text,
    firstseen timestamp,
    firsthostid int,
    PRIMARY KEY (customerguid, imagehash));

CREATE TABLE lsass_unsigned_imageloads (
    customerguid uuid,
    imagehash text,
    firstimagebasename text,
    firstseen timestamp,
    firsthostid int,
    PRIMARY KEY (customerguid, imagehash));

CREATE TABLE special_process_unsigned_imageloads (
    customerguid uuid,
    processbasename text,
    imagehash text,
    firstimagebasename text,
    firstseen timestamp,
    firsthostid int,
    PRIMARY KEY ((customerguid, processbasename), imagehash));

CREATE TABLE unknown_imageflags_detections (
    customerguid uuid,
    imagehash text,
    eventtimeuuid timeuuid,
    bucketname text,
    PRIMARY KEY (customerguid, imagehash, eventtimeuuid));

CREATE TABLE unknown_yarastatus_imageloads (
    imagehash text,
    customerguid uuid,
    processguid uuid,
    eventtimeuuid timeuuid,
    processbasename text,
    imagebasename text,
    pid int,

    processstarttime timeuuid,
	imagefullpath text,
    usersid text,
    userstring text,
    hostid int,
    hostname text,
    systemguid uuid,

    PRIMARY KEY (imagehash, customerguid, processguid));


// using this table to update the binary_global status after a customer ruleset changes
CREATE TABLE yara_imageload_hits (
    customerguid uuid,
    imagehash text,
    rulematch text,
    ruletime timestamp,
    PRIMARY KEY (customerguid, imagehash, rulematch));

CREATE TABLE safe_binaries (
    customerguid uuid,
    imagehash text,
    added_at timestamp,
    note text,
    PRIMARY KEY (customerguid, imagehash));

CREATE TABLE basename_signature_counters (
    customerid int,
    basename text,
    signed counter,
    unsigned counter,
    microsoft counter,
    unknown counter,
    PRIMARY KEY ((customerid,basename)));

////////////////////////BINARY POLICY////////////////////////////////////

CREATE TABLE desired_binary_policies (
    customerguid uuid,
    imagehash text,
    imageflags int,
    reason text,

    // adding these fields here until ScyllaDb fixes MV problem
    // we should be fetching this from binaries_by_policyenabled (MV on binaries_by_companyname)
    certificatehash text,
    basenames set<text>,
    firstseen timestamp,
    downloaded boolean,
    policyblockexecution boolean,
    policyblocknetwork boolean,

    PRIMARY KEY (customerguid, imagehash)) WITH CACHING = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };

CREATE TABLE set_binary_policies (
    customerguid uuid,
    systemguid uuid,
    sessionguid uuid,
    imagehash text,
    imageflags int,
    PRIMARY KEY ((customerguid, systemguid, sessionguid), imagehash));

CREATE TABLE binary_policy_history (
    customerguid uuid,
    imagehash text,
    tstamp timeuuid,
    oldimageflags int,
    newimageflags int,
    reason text,
    PRIMARY KEY (customerguid, imagehash, tstamp))
    with clustering order by (imagehash asc, tstamp desc);


////////////////////////KEY-VALUE STORE////////////////////////////////////

CREATE TABLE keyvaluestore (
    customerguid uuid,
    key text,
    value blob,
    PRIMARY KEY (customerguid, key));

//////////////////////// YARA PROCESS MEMORY SEARCH ////////////////////////

CREATE TABLE yaraexpressions (
    customerguid uuid,
    base64expression text,
    createtime timestamp,
    description text,
    compiledexpression blob,
    blockmatches boolean,
    active boolean,
    target text,
    PRIMARY KEY (customerguid, base64expression));

CREATE TYPE yaramatch (
    base bigint,
    offset bigint,
    data blob,
    length int
);

CREATE TABLE yararesults (
    customerguid uuid,
    systemguid uuid,
    actionid bigint,
    protopid bigint,
    tstamp timeuuid,
    base64expression text,
    matches list<frozen<yaramatch>>,
    processguid uuid,
    processbasename text,
    hostid int,
    hostname text,
    groupname text,
    groupactionid bigint,
    PRIMARY KEY (customerguid, systemguid, actionid, protopid))
    with clustering order by (systemguid asc, actionid desc, protopid asc);

CREATE TABLE yarasearchcounters (
    customerguid uuid,
    actionid bigint,
    completedprocesses counter,
    matches counter,
    PRIMARY KEY (customerguid, actionid)) WITH CLUSTERING ORDER BY (actionid DESC);

CREATE TABLE yarabinarysearchqueue (
    customerguid uuid,
    actionid bigint,
    imagehash text,
    filepath text,
    PRIMARY KEY ((customerguid, actionid), imagehash));

CREATE TABLE yarabinarysearchresults (
    customerguid uuid,
    actionid bigint,
    base64expression text,
    compiledexpression blob,
    filescancount bigint,
    matches set<text>,
    PRIMARY KEY (customerguid, actionid));
