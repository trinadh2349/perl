package FTFCU::P2P::ZOE;

use 5.016;
use warnings;
use strict;

use Carp;
use DateTime;

our $VERSION = 1.08;

sub new
{
    my ( $class, $args ) = @_;
    
    my $self = bless {}, $class;
    
    $self->{maxThread} = $args->{maxThread};
    $self->{threadId} = $args->{threadId};
    
    return $self;
}

sub processZoeRecords
{
    my( $self, $maxThread, $threadId, $zoeData ) = @_;
    
    my $maxRows = 1000;
    my $rowsProcessed = 0;

    my $sql = _getSql();
    
    my $sthP2pCust = $self->{dbh}->{p2p}->prepare_cached( $sql->{p2pCustOrg} );
    $sthP2pCust->execute();
    
    my $p2pCust = $sthP2pCust->fetchall_hashref('persnbr');
    
    $sthP2pCust->finish;
    
    my $sthTax = $self->{dbh}->{dna}->prepare_cached( $sql->{cardTaxRptForPers} );
    $sthTax->execute( $maxThread, $threadId );
    
    while ( my $zoeTaxRptForPers = $sthTax->fetchall_arrayref( [], $maxRows ) ) {
        foreach my $r ( @{ $zoeTaxRptForPers} ){
            my $line = buildDetailRecord( $r, $p2pCust );
            push @{ $zoeData }, $line;
        }
    }
    
    $sthTax->finish;
    
    my $sthOwn = $self->{dbh}->{dna}->prepare_cached( $sql->{cardOwnPers} );
    $sthOwn->execute( $maxThread, $threadId );
    
    while ( my $zoeOwnPers = $sthOwn->fetchall_arrayref( [], $maxRows ) ){
        foreach my $r ( @{ $zoeOwnPers} ){
            my( $line ) = buildDetailRecord( $r, $p2pCust );
            push @{ $zoeData }, $line;           
        }        
    }
    
    $sthOwn->finish;
    
    my $sthTaxNoCard =  $self->{dbh}->{dna}->prepare_cached( $sql->{noCardTaxRptForPers} );
    $sthTaxNoCard->execute( $maxThread, $threadId );
    
    while ( my $zoeOwnPers = $sthTaxNoCard->fetchall_arrayref( [], $maxRows ) ){
        foreach my $r ( @{ $zoeOwnPers} ){
            my( $line ) = buildDetailRecord( $r, $p2pCust );
            push @{ $zoeData }, $line;           
        }        
    }
    
    $sthTaxNoCard->finish;
    
    my $sthOwnNoCard = $self->{dbh}->{dna}->prepare_cached( $sql->{noCardOwnPers} );
    $sthOwnNoCard->execute( $maxThread, $threadId );
    
    while ( my $zoeOwnPers = $sthOwnNoCard->fetchall_arrayref( [], $maxRows ) ){
        foreach my $r ( @{ $zoeOwnPers} ){
            my( $line ) = buildDetailRecord( $r, $p2pCust );
            push @{ $zoeData }, $line;           
        }        
    }
    
    $sthOwnNoCard->finish;
    
    my $sthCardOwnPersOrg = $self->{dbh}->{dna}->prepare_cached( $sql->{cardOwnPersOrg} );
    $sthCardOwnPersOrg->execute( $maxThread, $threadId );
    
    while ( my $zoeOwnPers = $sthCardOwnPersOrg->fetchall_arrayref( [], $maxRows ) ){
        foreach my $r ( @{ $zoeOwnPers} ){
            my( $line ) = buildDetailRecord( $r, $p2pCust );
            push @{ $zoeData }, $line;           
        }             
    }
    
    $sthCardOwnPersOrg->finish;
    
    my $sthOrg = $self->{dbh}->{dna}->prepare_cached( $sql->{org} );
    $sthOrg->execute( $maxThread, $threadId );
    
    while ( my $zoeOwnOrg = $sthOrg->fetchall_arrayref( [], $maxRows ) ){
        foreach my $r ( @{ $zoeOwnOrg} ){
            my( $line ) = buildDetailRecord( $r, $p2pCust, 1 );
            push @{ $zoeData }, $line;           
        }        
    }
    
    $sthOrg->finish;
    
    return;
}

sub buildCDERecord
{
    my ( $self, $args ) = @_;
    
    return join '|', qw(
        CDE0380 CDE0377 CDE0276 CDE0157 CDE0557 CDE0014 CDE0011 CDE1023 CDE0019 CDE1024        
        CDE1025 CDE0023 CDE0029 CDE0032 CDE0033 CDE0036 CDE0055 CDE0056 CDE0077 CDE0100
        CDE1026 CDE0141 CDE0145 CDE0166 CDE0175 CDE0182 CDE0192 CDE0199 CDE0206 CDE0215
        CDE0216 CDE0219 CDE0222 CDE0227 CDE0233 CDE0277 CDE1027 CDE0238 CDE0283 CDE0284
        CDE0290 CDE0299 CDE0309 CDE0319 CDE0320 CDE0321 CDE0322 CDE0323 CDE0324 CDE0334
        CDE0345 CDE0354 CDE0408 CDE0409 CDE0802 CDE1275 CDE1271 CDE1272 CDE1273 CDE1274
        CDE0010    
    );
}

sub buildHeaderRecord
{
    my ( $self, $args ) = @_;
    
   my @headerRecAry;
   
   push @headerRecAry, 1,$args->{fileType};   
   push @headerRecAry,
    $args->{test} eq 'Y'
        ? '03'
        : '01';   
   
   push @headerRecAry, 'FTF';
   
   my $headerRecord = join '|', @headerRecAry;
   
   return $headerRecord;
}

sub buildDetailRecord{
    
    my ( $recordAry, $p2pCust, $isOrg ) = @_;
    
    my $persnbr = $recordAry->[1];
    my @lineAry = @{$recordAry}[0 .. 1]; # cardnbr, persnbr
    $lineAry[2] = (
        ! $isOrg && $p2pCust->{$persnbr} && $p2pCust->{$persnbr}->{CXCCustomerID}
            ? $p2pCust->{$persnbr}->{CXCCustomerID}
            : $persnbr
    ); # cxcCustomerId
    
    push @lineAry, @{$recordAry}[2 .. 12]; # acctnbr thru CDE0077
    
    if ( ! $isOrg && $p2pCust->{$persnbr} && $p2pCust->{$persnbr}->{registeredEmail} ){
        push @lineAry, $p2pCust->{$persnbr}->{registeredEmail}, 1;
    } 
    else{
        push @lineAry, $recordAry->[13], 0;
    } # email (CDE0100), CDE1026
    
    push @lineAry, @{ $recordAry }[14 .. 15];  # routing number CDE0141, aba number CDE0145 
    
    my $idAry = parseId( $recordAry->[16], $isOrg ); 
    
    push @lineAry, @{$idAry}[0 .. 5]; # CDE0166 ID expire date - CDE0206 persidtypcd
    
    push @lineAry, @{$recordAry}[17 .. 22]; # D.O.B. CDE0215 - middle name CDE0233
    
    if ( $p2pCust->{$persnbr} && $p2pCust->{$persnbr}->{registeredPhone} ){
        push @lineAry, $p2pCust->{$persnbr}->{registeredPhone}, 1;
    }
    else{
        push @lineAry, $recordAry->[23], 0;                
    } # CDE0277 - phone number, CDE1027 - Boolean field
    
    push @lineAry, @{$recordAry}[24 .. 47]; # CDE0238  - CDE1274
    
    push @lineAry, $recordAry->[49]; # CDE0010 curracctstatcd
    
    my( $line ) = join '|', map{ defined $_ ? $_ : '' }@lineAry;

    return $line;
}

sub buildTrailerRecord
{
    my ( $self, $args, $zoeData, $fileStatAry ) = @_;
    
    my $fileEpoch = $fileStatAry->[10];
    my $fileCreateDate = DateTime->today(time_zone => 'America/Los_Angeles')->ymd('');
    my $fileCreateTime = DateTime->from_epoch(time_zone => 'America/Los_Angeles', epoch => $fileEpoch )->hms('');
    my $fileMs = DateTime->from_epoch(time_zone => 'America/Los_Angeles', epoch => $fileEpoch )->millisecond();
    
    die "Record Count argument is undefined"
        if ! $args->{recordCt};
        
    die "Account Hash argument is undefined"
        if ! $args->{acctHash};
    
    my $fileAcctHash = $args->{acctHash};     
    my $fileRecordCt = $args->{recordCt};
    my $fileAddCt = $args->{added} // 0;
    my $fileChangeCt = $args->{changed} // 0;
    my $fileDeleteCt = $args->{delete} // 0; 
    
    push my @trailerAry,
        9,
        $args->{fileType},
        $args->{test} eq 'Y'
            ? '03'
            : '01',
        'FTF';
    
    push my @cdeVals, qw(
        CDE0083 CDE0084 CDE0110 CDE0111 CDE0120
        CDE0121 CDE0123 CDE0133 CDE0139 CDE0151
        CDE0165 CDE0418 CDE0419 CDE0429 CDE0430
        CDE0467 CDE0674 CDE0676 CDE0811
    );
    
    push my @trailerVals,
        $fileCreateDate,
        $fileCreateTime . $fileMs,
        $fileAcctHash,
        $fileAddCt,
        $fileChangeCt,
        $fileDeleteCt,
        '',
        $fileRecordCt,
        'ZOE',
        '', # FI bank id
        '', # FI region
        '', # xfer date - unknowable - happens downstream via Appworx
        '', # xfer time - same deal
        '', # process end date - wtf is this?  EWS does the processing so they'd have to populate this field
        '', # process end time - same deal
        $fileEpoch,
        '', # source file name - not applicable
        'A',
        ''; # client id - per EWS reserved for future use
         
    push @trailerAry, map{ "$cdeVals[$_]:$trailerVals[$_]" }( keys @cdeVals );
    
    my $trailerRec = join '|', @trailerAry;
    
    return $trailerRec;
}

sub parseId
{
    my ( $idRecordStr, $isOrg ) = @_;
    
    my @idAry;
    
    if ( ! $isOrg && $idRecordStr ){
        my @idRowAry;
        
        if ( $idRecordStr =~ /\|/ ){
            @idRowAry = map{ [ split /:/, $_ ] }( split /\|/, $idRecordStr );    
        }
        else{
            push @idRowAry, [ split /:/, $idRecordStr ]; 
        }
        
        my @usaIdAry = map{ $_->[3] && $_->[3] eq 'USA' ? $_ : () }@idRowAry; # issuer ctrycd = 'USA'
        
        # use a US issued ID if one exists
        if ( scalar @usaIdAry > 0 ){
            foreach my $usId ( @usaIdAry ){
                @idAry = @{ $usId } if $usId->[4]; # assign the first US Issued ID that has a non-null ID number     
            }
        }
        else{
            my @foreignIdAry =  map{ $_->[3] && $_->[3] ne 'USA' ? $_ : () }@idRowAry;
            foreach my $forId( @foreignIdAry ){
                @idAry = @{ $forId } if $forId->[4]; # assign the first foreign ID that has a non-null ID number
            }
        }
        
    }

    if ( scalar @idAry == 0 ){ # assign all 'blanks' if is Org or no non-null ID number found
        push @idAry,'','','','','','','';    
    }
    
    return \@idAry;
}

sub getZoeFileHash
{
    my ( $self, $fh ) = @_;
    
    my %fileHash;
    my $acctHash;
    
    while ( <$fh> ){
        chomp;
        
        my $line = $_;
        next if $line !~ /^6/;
        
        my @lineAry = split /\|/, $line;
        
        $acctHash += $lineAry[8];
        
        my $key = $self->getKey($line);
        
        $fileHash{$key} = join '|', @lineAry[5 .. 59];
   }
    
    return ( \%fileHash, $acctHash );
}

sub getKey
{
    my ( $self, $line ) = @_;
    
    chomp;
       
    my @lineAry = split /\|/, $line;
    
    my $persnbr = $lineAry[6];
    my $acctnbr = $lineAry[8];
    my $cardnbr = $lineAry[5];
    
    
    my $key = (
        $cardnbr
            ? join( '|', $persnbr,$acctnbr,$cardnbr )
            : join( '|', $persnbr, $acctnbr )
    );
    
    $key =~ s/\|+$//;
    
    return $key;   
}

sub _getSql
{
    
    my $with = qq(
        WITH adr AS (
            SELECT DISTINCT
                persnbr,
                LISTAGG(text, ' ')
                    WITHIN GROUP (
                        ORDER BY ar.linenbr
                ) AS address,
                addr.cityname,
                addr.ctrycd,
                addr.statecd,
                addr.zipcd,
                addr.zipsuf
            FROM (
                SELECT DISTINCT
                    addrnbr,
                    linenbr,
                    text
                FROM addrline a
                JOIN addrlinetyp  al
                    ON a.addrlinetypcd = al.addrlinetypcd
            WHERE al.mailaddryn = 'Y'
            ORDER BY al.addrlinetypseq, a.linenbr
            ) ar
            JOIN persaddruse pa
                ON ar.addrnbr = pa.addrnbr
                AND pa.addrusecd = 'PRI'
                AND pa.inactivedate IS NULL
            JOIN addr
                ON pa.addrnbr = addr.addrnbr
            GROUP BY persnbr, addr.ctrycd, addr.cityname, addr.statecd, addr.zipcd, addr.zipsuf
        ),
        ash AS(
            SELECT DISTINCT
                a.acctnbr,
                a.curracctstatcd
            FROM acct a
            JOIN acctacctstathist ash
                ON a.acctnbr = ash.acctnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('PSA','BRHS','IAFT','SCUS','SSA','SPA','HCA','DSA','CUST','PCKA','FCPC','CKA','FCKA','RCKA') --consumer, non-retirement, non-Chargeoff
            AND a.taxrptforpersnbr NOT IN(1094014,1093153,1379371)
            AND (
                a.curracctstatcd = 'ACT'
                OR(
                    a.curracctstatcd = 'CLS'
                    AND EXISTS(
                        SELECT DISTINCT 1
                        FROM acctacctstathist ashz
                        WHERE ashz.acctnbr = ash.acctnbr
                        AND ashz.acctstatcd = 'CLS'
                        AND ashz.effdatetime >= TRUNC(SYSDATE - 365)
                    )
                )
            )
        )   
    );
    
    my %sql = (
        cardTaxRptForPers    =>  qq(                          
            $with
            
            SELECT
                '' extcardnbr, -- ca.extcardnbr debit card nbr no longer required
                a.taxrptforpersnbr persnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'DSA'
                    --AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptforpersnbr = aa.taxrptforpersnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'DSA'
                        --AND aa.curracctstatcd = 'ACT'
                    )
                    AND a.taxrptforpersnbr = d.taxrptforpersnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                      AND a.taxrptforpersnbr <> aarp.persnbr
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'CC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'CS'
                    ELSE NULL
                END accttyp,
                '0' businessindicator,
                NULL businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM persaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.persnbr = p.persnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
                (
                    SELECT
                        LISTAGG(
                            TO_CHAR(pi.expiredate,'YYYYMMDD')
                            || ':' ||
                            TO_CHAR(pi.issuedate,'YYYYMMDD')
                            || ':' ||
                            pi.ctrycd
                            || ':' ||
                            pi.statecd
                            || ':' ||
                            pi.idnbr
                            || ':' ||
                            DECODE(
                                pi.persidtypcd,
                                   1,0, --USA DL
                                   2,4, --State ID
                                   3,2, --Passport
                                   6,1, --USA Mil
                                   8,3, --Resident Alien
                                   16,2, --Passport Card
                                   NULL
                            )
                            || ':' ||
                            i.persidtypdesc,
                            '|'
                        ) WITHIN GROUP( ORDER BY pi.persnbr ) id_row
                    FROM persid pi
                    JOIN persidtyp i
                        ON pi.persidtypcd = i.persidtypcd
                    WHERE i.persidtypcd IN(1,2,3,6,8,16)
                    AND pi.persnbr = a.taxrptforpersnbr
                    GROUP BY pi.persnbr
                ) idrow,
                TO_CHAR(p.datebirth,'YYYYMMDD') datebirth,
                NVL2(p.datedeath,'Y','N') deceased,
               (
                  p.lastname || ',' || p.firstname || 
                  DECODE( p.mdlinit, NULL, ',', (',' || p.mdlinit ||'.')) || -- middle initial if present
                  DECODE( p.suffix, NULL, ',', (',' || p.suffix ||'.,')) -- suffix if present
               ) name,
                p.firstname,
                p.lastname,
                p.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM persphone pp
                    WHERE a.taxrptforpersnbr = pp.persnbr
                    AND phoneusecd = 'PER'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                SUBSTR( pack_encrypt.func_decrypt( taxid, pack_BANK.func_GETTAXIDKEYVAL ), 0, 9 )taxid,
                1 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC09' individualclassification,
                'AC09' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'TAX' querysource,
                ash.curracctstatcd
            FROM acct a
            JOIN pers p
                ON a.taxrptforpersnbr = p.persnbr
            JOIN acctagreementpers aap
                ON a.acctnbr = aap.acctnbr AND a.taxrptforpersnbr = aap.persnbr
            JOIN cardagreement ca
                ON aap.agreenbr = ca.agreenbr
            JOIN cardmember cm
                ON ca.agreenbr = cm.agreenbr
                AND ca.ownerpersnbr = cm.persnbr
            JOIN cardmemberissue cmi
                ON cm.agreenbr = cmi.agreenbr
                AND cm.membernbr = cmi.membernbr
                AND cm.currissuenbr = cmi.issuenbr
            JOIN ash ON a.acctnbr = ash.acctnbr
            LEFT JOIN adr
                ON a.taxrptforpersnbr = adr.persnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('PSA','BRHS','IAFT','SCUS','SSA','SPA','HCA','DSA','CUST','PCKA','FCPC','CKA','FCKA','RCKA') --consumer, non-retirement, non-Chargeoff
            AND a.taxrptforpersnbr IS NOT NULL
            AND p.persnbr NOT IN(1094014,1093153,1379371)
            AND a.mjaccttypcd IN('CK','SAV')
            AND a.curracctstatcd IN('ACT')
            AND aap.inactivedate IS NULL
            AND ca.agreetypcd IN('MBD','MDBT','MCWD')
            AND aap.persnbr = ca.ownerpersnbr
            AND cmi.expiredate >= TRUNC(SYSDATE)
            AND cmi.currstatuscd = 'ACT'
            AND cm.membernbr = (
                SELECT
                    MAX(cmz.membernbr)
                FROM cardmember cmz
                WHERE cmz.agreenbr = cm.agreenbr
                AND cmz.persnbr = cm.persnbr
            )
            AND MOD(a.taxrptforpersnbr, ?) = ?
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'DSA'
                AND aa.curracctstatcd = 'ACT'
                AND a.taxrptforpersnbr = aa.taxrptforpersnbr
            )
        ),
        cardOwnPers    =>  qq(
            $with
                
            SELECT
                '' extcardnbr, -- ca.extcardnbr debit card nbr no longer required
                arp.persnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'DSA'
                    AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptforpersnbr = aa.taxrptforpersnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'DSA'
                        AND aa.curracctstatcd = 'ACT'
                    )
                    AND arp.persnbr = d.taxrptforpersnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1 
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                      AND a.taxrptforpersnbr <> aarp.persnbr
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'CC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'CS'
                    ELSE NULL
                END accttyp,
                '0' businessindicator,
                NULL businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM persaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.persnbr = p.persnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
                (
                    SELECT
                        LISTAGG(
                            TO_CHAR(pi.expiredate,'YYYYMMDD')
                            || ':' ||
                            TO_CHAR(pi.issuedate,'YYYYMMDD')
                            || ':' ||
                            pi.ctrycd
                            || ':' ||
                            pi.statecd
                            || ':' ||
                            pi.idnbr
                            || ':' ||
                            DECODE(
                                pi.persidtypcd,
                                   1,0, --USA DL
                                   2,4, --State ID
                                   3,2, --Passport
                                   6,1, --USA Mil
                                   8,3, --Resident Alien
                                   16,2, --Passport Card
                                   NULL
                            )
                            || ':' ||
                            i.persidtypdesc,
                            '|'
                        ) WITHIN GROUP( ORDER BY pi.persnbr ) id_row
                    FROM persid pi
                    JOIN persidtyp i
                        ON pi.persidtypcd = i.persidtypcd
                    WHERE i.persidtypcd IN(1,2,3,6,8,16)
                    AND pi.persnbr = arp.persnbr
                    GROUP BY pi.persnbr
                ) idrow,
                TO_CHAR(p.datebirth,'YYYYMMDD') datebirth,
                NVL2(p.datedeath,'Y','N') deceased,
               (
                  p.lastname || ',' || p.firstname || 
                  DECODE( p.mdlinit, NULL, ',', (',' || p.mdlinit ||'.')) || -- middle initial if present
                  DECODE( p.suffix, NULL, ',', (',' || p.suffix ||'.,')) -- suffix if present
               ) name,
                p.firstname,
                p.lastname,
                p.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM persphone pp
                    WHERE arp.persnbr = pp.persnbr
                    AND phoneusecd = 'PER'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                SUBSTR( pack_encrypt.func_decrypt( taxid, pack_BANK.func_GETTAXIDKEYVAL ), 0, 9 )taxid,
                1 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC11' individualclassification,
                'AC09' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'OWN' querysource,
                ash.curracctstatcd
            FROM acctacctrolepers arp
            JOIN acct a
            	ON arp.acctnbr = a.acctnbr
            JOIN pers p
                ON arp.persnbr = p.persnbr
            JOIN acctagreementpers aap
                ON arp.acctnbr = aap.acctnbr AND arp.persnbr = aap.persnbr --a.taxrptforpersnbr = aap.persnbr
            JOIN cardagreement ca
                ON aap.agreenbr = ca.agreenbr
            JOIN cardmember cm
                ON ca.agreenbr = cm.agreenbr
                AND ca.ownerpersnbr = cm.persnbr
            JOIN cardmemberissue cmi
                ON cm.agreenbr = cmi.agreenbr
                AND cm.membernbr = cmi.membernbr
                AND cm.currissuenbr = cmi.issuenbr
            JOIN ash ON a.acctnbr = ash.acctnbr
            LEFT JOIN adr
                ON arp.persnbr = adr.persnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('PSA','BRHS','IAFT','SCUS','SSA','SPA','HCA','DSA','CUST','PCKA','FCPC','CKA','FCKA','RCKA') --consumer, non-retirement, non-Chargeoff
            AND a.taxrptforpersnbr IS NOT NULL
            AND p.persnbr NOT IN(1094014,1093153,1379371)
            AND arp.acctrolecd = 'OWN'
            AND arp.persnbr <> a.taxrptforpersnbr
            AND aap.inactivedate IS NULL
            AND ca.agreetypcd IN('MBD','MDBT','MCWD')
            AND aap.persnbr = ca.ownerpersnbr
            AND cmi.expiredate >= TRUNC(SYSDATE)
            AND cmi.currstatuscd = 'ACT'
            AND cm.membernbr = (
                SELECT
                    MAX(cmz.membernbr)
                FROM cardmember cmz
                WHERE cmz.agreenbr = cm.agreenbr
                AND cmz.persnbr = cm.persnbr
            )
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'DSA'
                AND aa.curracctstatcd = 'ACT'
                AND arp.persnbr = aa.taxrptforpersnbr
            )
            AND MOD(arp.persnbr, ?) = ?
        ),
        noCardTaxRptForPers =>  qq(
            $with
                
            SELECT
                '' extcardnbr,
                a.taxrptforpersnbr persnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'DSA'
                    AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptforpersnbr = aa.taxrptforpersnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'DSA'
                        AND aa.curracctstatcd = 'ACT'
                    )
                    AND a.taxrptforpersnbr = d.taxrptforpersnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1 
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                      AND a.taxrptforpersnbr <> aarp.persnbr
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'CC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'CS'
                    ELSE NULL
                END accttyp,
                '0' businessindicator,
                NULL businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM persaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.persnbr = p.persnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
                (
                    SELECT
                        LISTAGG(
                            TO_CHAR(pi.expiredate,'YYYYMMDD')
                            || ':' ||
                            TO_CHAR(pi.issuedate,'YYYYMMDD')
                            || ':' ||
                            pi.ctrycd
                            || ':' ||
                            pi.statecd
                            || ':' ||
                            pi.idnbr
                            || ':' ||
                            DECODE(
                            pi.persidtypcd,
                               1,0, --USA DL
                               2,4, --State ID
                               3,2, --Passport
                               6,1, --USA Mil
                               8,3, --Resident Alien
                               16,2, --Passport Card
                               NULL
                            )
                            || ':' ||
                            i.persidtypdesc,
                            '|'
                        ) WITHIN GROUP( ORDER BY pi.persnbr ) id_row
                    FROM persid pi
                    JOIN persidtyp i
                        ON pi.persidtypcd = i.persidtypcd
                    WHERE i.persidtypcd IN(1,2,3,6,8,16)
                    AND pi.persnbr = a.taxrptforpersnbr
                    GROUP BY pi.persnbr
                ) idrow,
                TO_CHAR(p.datebirth,'YYYYMMDD') datebirth,
                NVL2(p.datedeath,'Y','N') deceased,
               (
                  p.lastname || ',' || p.firstname || 
                  DECODE( p.mdlinit, NULL, ',', (',' || p.mdlinit ||'.')) || -- middle initial if present
                  DECODE( p.suffix, NULL, ',', (',' || p.suffix ||'.,')) -- suffix if present
               ) name,
                p.firstname,
                p.lastname,
                p.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM persphone pp
                    WHERE a.taxrptforpersnbr = pp.persnbr
                    AND phoneusecd = 'PER'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                SUBSTR( pack_encrypt.func_decrypt( taxid, pack_BANK.func_GETTAXIDKEYVAL ), 0, 9 )taxid,
                1 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC09' individualclassification,
                'AC09' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'TAX_NO_CARD' querysource,
                ash.curracctstatcd
            FROM acct a
            JOIN pers p
                ON a.taxrptforpersnbr = p.persnbr
            JOIN ash ON a.acctnbr = ash.acctnbr
            LEFT JOIN adr
                ON a.taxrptforpersnbr = adr.persnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('PSA','BRHS','IAFT','SCUS','SSA','SPA','HCA','DSA','CUST','PCKA','FCPC','CKA','FCKA','RCKA') --consumer, non-retirement, non-Chargeoff
            AND a.taxrptforpersnbr IS NOT NULL
            AND p.persnbr NOT IN(1094014,1093153,1379371)
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'DSA'
                AND aa.curracctstatcd = 'ACT'
                AND a.taxrptforpersnbr = aa.taxrptforpersnbr
            )
            AND NOT EXISTS(
                SELECT 1
                FROM cardagreement ca
                WHERE ownerpersnbr = a.taxrptforpersnbr
                AND ca.agreetypcd IN( 'MBD','MDBT','MCWD' )
            )
            AND MOD(a.taxrptforpersnbr, ?) = ?            
        ),
        noCardOwnPers    =>  qq(
            $with
                
            SELECT
                '' extcardnbr,
                arp.persnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'DSA'
                    AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptforpersnbr = aa.taxrptforpersnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'DSA'
                        AND aa.curracctstatcd = 'ACT'
                    )
                    AND arp.persnbr = d.taxrptforpersnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1 
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                      AND a.taxrptforpersnbr <> aarp.persnbr
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'CC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'CS'
                    ELSE NULL
                END accttyp,
                '0' businessindicator,
                NULL businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM persaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.persnbr = p.persnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
                (
                    SELECT
                        LISTAGG(
                            TO_CHAR(pi.expiredate,'YYYYMMDD')
                            || ':' ||
                            TO_CHAR(pi.issuedate,'YYYYMMDD')
                            || ':' ||
                            pi.ctrycd
                            || ':' ||
                            pi.statecd
                            || ':' ||
                            pi.idnbr
                            || ':' ||
                            DECODE(
                                pi.persidtypcd,
                                   1,0, --USA DL
                                   2,4, --State ID
                                   3,2, --Passport
                                   6,1, --USA Mil
                                   8,3, --Resident Alien
                                   16,2, --Passport Card
                                   NULL
                            )
                            || ':' ||
                            i.persidtypdesc,
                            '|'
                        ) WITHIN GROUP( ORDER BY pi.persnbr ) id_row
                    FROM persid pi
                    JOIN persidtyp i
                        ON pi.persidtypcd = i.persidtypcd
                    WHERE i.persidtypcd IN(1,2,3,6,8,16)
                    AND pi.persnbr = arp.persnbr
                    GROUP BY pi.persnbr
                ) idrow,
                TO_CHAR(p.datebirth,'YYYYMMDD') datebirth,
                NVL2(p.datedeath,'Y','N') deceased,
               (
                  p.lastname || ',' || p.firstname || 
                  DECODE( p.mdlinit, NULL, ',', (',' || p.mdlinit ||'.')) || -- middle initial if present
                  DECODE( p.suffix, NULL, ',', (',' || p.suffix ||'.,')) -- suffix if present
               ) name,
                p.firstname,
                p.lastname,
                p.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM persphone pp
                    WHERE arp.persnbr = pp.persnbr
                    AND phoneusecd = 'PER'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                SUBSTR( pack_encrypt.func_decrypt( taxid, pack_BANK.func_GETTAXIDKEYVAL ), 0, 9 )taxid,
                1 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC11' individualclassification,
                'AC09' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'OWN_NO_CARD' querysource,
                ash.curracctstatcd
            FROM acctacctrolepers arp
            JOIN acct a
            	ON arp.acctnbr = a.acctnbr
            JOIN pers p
                ON arp.persnbr = p.persnbr
            JOIN ash ON a.acctnbr = ash.acctnbr
            LEFT JOIN adr
                ON arp.persnbr = adr.persnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('PSA','BRHS','IAFT','SCUS','SSA','SPA','HCA','DSA','CUST','PCKA','FCPC','CKA','FCKA','RCKA') --consumer, non-retirement, non-Chargeoff
            AND a.taxrptforpersnbr IS NOT NULL
            AND arp.acctrolecd = 'OWN'
            AND p.persnbr NOT IN(1094014,1093153,1379371)
            AND arp.persnbr <> a.taxrptforpersnbr
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'DSA'
                AND aa.curracctstatcd = 'ACT'
                AND arp.persnbr = aa.taxrptforpersnbr
            )
            AND NOT EXISTS(
                SELECT 1
                FROM cardagreement ca
                WHERE ownerpersnbr = arp.persnbr
                AND ca.agreetypcd IN( 'MBD','MDBT','MCWD' )
            )
            AND MOD(arp.persnbr, ?) = ?
        ),
        cardOwnPersOrg =>  qq(
            $with
                
            SELECT
                NULL, -- ca.extcardnbr debit card nbr no longer required
                arp.persnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'DSA'
                    AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptforpersnbr = aa.taxrptforpersnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'DSA'
                        AND aa.curracctstatcd = 'ACT'
                    )
                    AND arp.persnbr = d.taxrptforpersnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'BC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'BS'
                    ELSE NULL
                END accttyp,
                '1' businessindicator,
                o.orgname businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM persaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.persnbr = p.persnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
                (
                    SELECT
                        LISTAGG(
                            TO_CHAR(pi.expiredate,'YYYYMMDD')
                            || ':' ||
                            TO_CHAR(pi.issuedate,'YYYYMMDD')
                            || ':' ||
                            pi.ctrycd
                            || ':' ||
                            pi.statecd
                            || ':' ||
                            pi.idnbr
                            || ':' ||
                            DECODE(
                                pi.persidtypcd,
                                   1,0, --USA DL
                                   2,4, --State ID
                                   3,2, --Passport
                                   6,1, --USA Mil
                                   8,3, --Resident Alien
                                   16,2, --Passport Card
                                   NULL
                            )
                            || ':' ||
                            i.persidtypdesc,
                            '|'
                        ) WITHIN GROUP( ORDER BY pi.persnbr ) id_row
                    FROM persid pi
                    JOIN persidtyp i
                        ON pi.persidtypcd = i.persidtypcd
                    WHERE i.persidtypcd IN(1,2,3,6,8,16)
                    AND pi.persnbr = arp.persnbr
                    GROUP BY pi.persnbr
                ) idrow,
                TO_CHAR(p.datebirth,'YYYYMMDD') datebirth,
                NVL2(p.datedeath,'Y','N') deceased,
               (
                  p.lastname || ',' || p.firstname || 
                  DECODE( p.mdlinit, NULL, ',', (',' || p.mdlinit ||'.')) || -- middle initial if present
                  DECODE( p.suffix, NULL, ',', (',' || p.suffix ||'.,')) -- suffix if present
               ) name,
                p.firstname,
                p.lastname,
                p.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM persphone pp
                    WHERE arp.persnbr = pp.persnbr
                    AND phoneusecd = 'PER'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                SUBSTR( pack_encrypt.func_decrypt( taxid, pack_BANK.func_GETTAXIDKEYVAL ), 0, 9 )taxid,
                1 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC11' individualclassification,
                'AC02' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'CARDPERSORG' querysource,
                ash.curracctstatcd
            FROM acct a
            JOIN org o
            	ON a.taxrptfororgnbr = o.orgnbr
            JOIN acctacctrolepers arp
            	ON a.acctnbr = arp.acctnbr
            JOIN pers p
                ON arp.persnbr = p.persnbr
            JOIN acctagreementpers aap
                ON arp.acctnbr = aap.acctnbr AND arp.persnbr = aap.persnbr
            JOIN cardagreement ca
                ON aap.agreenbr = ca.agreenbr
            JOIN cardmember cm
                ON ca.agreenbr = cm.agreenbr
                AND ca.ownerpersnbr = cm.persnbr
            JOIN cardmemberissue cmi
                ON cm.agreenbr = cmi.agreenbr
                AND cm.membernbr = cmi.membernbr
                AND cm.currissuenbr = cmi.issuenbr
            JOIN ash ON a.acctnbr = ash.acctnbr
            LEFT JOIN adr
                ON arp.persnbr = adr.persnbr
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('SBSC','FBSS','CIAC','BCDC','BCFC','FBCC') --business, non-Chargeoff
            AND a.taxrptfororgnbr IS NOT NULL
            AND arp.acctrolecd = 'OWN'
            AND aap.inactivedate IS NULL
            AND ca.agreetypcd IN('MBD','MDBT','MCWD')
            AND aap.persnbr = ca.ownerpersnbr
            AND cmi.expiredate >= TRUNC(SYSDATE)
            AND cmi.currstatuscd = 'ACT'
            AND cm.membernbr = (
                SELECT
                    MAX(cmz.membernbr)
                FROM cardmember cmz
                WHERE cmz.agreenbr = cm.agreenbr
                AND cmz.persnbr = cm.persnbr
            )
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'DSA'
                AND aa.curracctstatcd = 'ACT'
                AND arp.persnbr = aa.taxrptforpersnbr
            )
            AND MOD(arp.persnbr, ?) = ?
        ),
        org         =>  qq(
            WITH adr AS (
                    SELECT DISTINCT
                        orgnbr,
                        LISTAGG(text, ' ')
                            WITHIN GROUP (
                                ORDER BY ar.linenbr
                        ) AS address,
                        addr.cityname,
                        addr.ctrycd,
                        addr.statecd,
                        addr.zipcd,
                        addr.zipsuf
                    FROM (
                        SELECT DISTINCT
                            addrnbr,
                            linenbr,
                            text
                        FROM addrline a
                        JOIN addrlinetyp  al
                            ON a.addrlinetypcd = al.addrlinetypcd
                    WHERE al.mailaddryn = 'Y'
                    ORDER BY al.addrlinetypseq, a.linenbr
                    ) ar
                    JOIN orgaddruse pa
                        ON ar.addrnbr = pa.addrnbr
                        AND pa.addrusecd = 'PRI'
                        AND pa.inactivedate IS NULL
                    JOIN addr
                        ON pa.addrnbr = addr.addrnbr
                    GROUP BY orgnbr, addr.ctrycd, addr.cityname, addr.statecd, addr.zipcd, addr.zipsuf
                ),
                ash AS(
                    SELECT DISTINCT
                        a.acctnbr,
                        a.curracctstatcd
                    FROM acct a
                    JOIN acctacctstathist ash
                        ON a.acctnbr = ash.acctnbr
                    WHERE a.mjaccttypcd IN('CK','SAV')
                    AND a.currmiaccttypcd IN('SBSC','FBSS','CIAC','BCDC','BCFC','FBCC') -- business
                    AND (
                        a.curracctstatcd = 'ACT'
                        OR(
                            a.curracctstatcd = 'CLS'
                            AND EXISTS(
                                SELECT DISTINCT 1
                                FROM acctacctstathist ashz
                                WHERE ashz.acctnbr = ash.acctnbr
                                AND ashz.acctstatcd = 'CLS'
                                AND ashz.effdatetime >= TRUNC(SYSDATE - 365)
                            )
                        )
                    )
                ),
               business_auth_signers AS (
                	SELECT DISTINCT
                		a.acctnbr,
                     o.orgnbr,
                     ap.persnbr,
                     p.firstname,
                     p.lastname,
                     p.mdlname,
                     p.mdlinit,
                     p.suffix

                  FROM acct a

                  JOIN org o
                     ON a.taxrptfororgnbr = o.orgnbr

                  JOIN acctacctrolepers ap
                     ON a.acctnbr = ap.acctnbr

                  JOIN pers p
                     ON ap.persnbr = p.persnbr

                  WHERE ap.acctrolecd IN ('AUTH', 'SIGN')
                )

            SELECT
                '' extcardnbr,
                a.taxrptfororgnbr orgnbr,
                a.acctnbr,
                NULL MICR_Current,
                NULL MICR_Old,
                TO_CHAR(a.contractdate,'YYYYMMDD') contractdate,
                (
                    SELECT DISTINCT
                        TO_CHAR(d.contractdate,'YYYYMMDD')
                    FROM acct d
                    WHERE d.mjaccttypcd = 'SAV'
                    AND d.currmiaccttypcd = 'SBSC'
                    AND d.curracctstatcd = 'ACT'
                    AND d.contractdate = (
                        SELECT
                            MAX(aa.contractdate)
                        FROM acct aa
                        WHERE d.taxrptfororgnbr = aa.taxrptfororgnbr
                        AND aa.mjaccttypcd = 'SAV'
                        AND aa.currmiaccttypcd = 'SBSC'
                        AND aa.curracctstatcd = 'ACT'
                    )
                    AND a.taxrptfororgnbr = d.taxrptfororgnbr
                    AND ROWNUM = 1
                ) dsa_contractdate,
                (
                  SELECT
                      COUNT(persnbr) + 1
                      FROM acctacctrolepers aarp
                      WHERE a.acctnbr = aarp.acctnbr
                      AND aarp.acctrolecd = 'OWN'
                      AND aarp.inactivedate IS NULL
                ) acctsegmentct,
                'A' acctsegtyp,
                CASE
                    WHEN a.mjaccttypcd = 'CK'
                    THEN 'BC'
                    WHEN a.mjaccttypcd = 'SAV'
                    THEN 'BS'
                    ELSE NULL
                END accttyp,
                '1' businessindicator,
                o.orgname businessname,
                NULL contributionsource,
                (
                    SELECT e.text
                    FROM orgaddruse c
                    LEFT JOIN addr d on c.addrnbr = d.addrnbr
                    LEFT JOIN addrline e on d.addrnbr = e.addrnbr
                    WHERE c.addrusecd = 'EML1'
                    AND (c.inactivedate is null or c.inactivedate > sysdate)
                    AND e.linenbr = 1
                    AND e.text != 'NOEMAIL\@1STTECH.COM'
                    AND e.text != 'Email Address'
                    AND c.orgnbr = o.orgnbr
                    AND ROWNUM = 1
                ) as email,
                '321180379' rtnbr,
                '321180379' abanbr,
				'' idrow,
                '' datebirth,
                '' deceased,
					(
                  bo.lastname || ',' || bo.firstname ||
                  DECODE( bo.mdlinit, NULL, ',', (',' || bo.mdlinit ||'.')) || -- middle initial if present
                  DECODE( bo.suffix, NULL, ',', (',' || bo.suffix ||'.,')) -- suffix if present
               ) owner_auth_signer_name,
                bo.firstname,
                bo.lastname,
                bo.mdlname,
                (
                    SELECT
                        areacd || exchange || phonenbr phonenbr
                    FROM orgphone oo
                    WHERE a.taxrptfororgnbr = oo.orgnbr
                    AND phoneusecd = 'BUS'
                    AND ROWNUM = 1
                ) phone,
                'AH',
                NULL intldialcd,
                'M',
                adr.cityname,
                adr.ctrycd,
                adr.statecd,
                adr.address,
                NULL altaddr1,
                NULL altaddr2,
                NULL altaddr3,
                NULL altaddr4,
                NULL altaddr5,
                'P' addrtyp,
                adr.zipcd,
                NULL,
                pack_encrypt.func_decrypt ( ot.taxid, pack_BANK.func_GETTAXIDKEYVAL ) taxid,
                2 taxidtyp,
                NULL suffix,
                NULL prefix,
                NULL businesscd,
                NULL businessdba,
                'IC09' individualclassification,
                'AC02' acctclassification,
                (
                    SELECT
                        TO_CHAR(effdatetime,'YYYYMMDD')
                    FROM acctacctstathist ash
                    WHERE a.acctnbr = ash.acctnbr
                    AND ash.acctstatcd = 'CLS'
                    AND ROWNUM = 1
                ) acctclosedate,
                'ORG' querysource,
                ash.curracctstatcd
                
            FROM acct a
            
            JOIN org o
                ON a.taxrptfororgnbr = o.orgnbr
                
            JOIN orgtaxid ot
                ON o.orgnbr = ot.orgnbr
                
            JOIN ash
               ON a.acctnbr = ash.acctnbr
               
            LEFT JOIN adr
                ON a.taxrptfororgnbr = adr.orgnbr
            
            JOIN business_auth_signers bo
            	ON a.acctnbr = bo.acctnbr
            	AND a.taxrptfororgnbr = bo.orgnbr
            
                
            WHERE a.mjaccttypcd IN('CK','SAV')
            AND a.currmiaccttypcd IN('SBSC','FBSS','CIAC','BCDC','BCFC','FBCC') -- business
            AND a.taxrptfororgnbr IS NOT NULL
            AND EXISTS(
                SELECT 1
                FROM acct aa
                WHERE aa.currmiaccttypcd = 'SBSC'
                AND aa.curracctstatcd = 'ACT'
                AND a.taxrptfororgnbr = aa.taxrptfororgnbr
            )
            AND MOD(a.taxrptfororgnbr, ?) = ?            
        ),
        p2pCustOrg  =>  qq(
            SELECT
                c.OSICoreId persnbr,
                LOWER(c.CXCCustomerID) as CXCCustomerID,
                c.OrgId,
                (
                    SELECT TOP 1
                        t.MemberToken
                    FROM Token t
                    WHERE c.Id = t.CustomerId
                    AND t.[Type] = 'E'
                ) registeredEmail,
                (
                    SELECT TOP 1
                        t.MemberToken
                    FROM Token t
                    WHERE c.Id = t.CustomerId
                    AND t.[Type] = 'P'
                ) registeredPhone
            FROM Customer c              
        ),
    );
    
    return \%sql;
}

# saved for possible future use
sub initFileRecordSequence
{
    my ( $self, $dbh ) = @_;
    
    my $sth = $dbh->table_info( '%', "OSIUPDATE", '%', "SEQUENCE" );
    
    if ( $sth->fetchall_hashref('TABLE_NAME')->{'P2P_ZOE_SEQ'} ){
        $dbh->do("DROP SEQUENCE osiupdate.p2p_zoe_seq");       
        $dbh->do("CREATE SEQUENCE osiupdate.p2p_zoe_seq START WITH 1 INCREMENT BY 1");
    }
    else{
        $dbh->do("CREATE SEQUENCE osiupdate.p2p_zoe_seq START WITH 1 INCREMENT BY 1");
    }
    
    $dbh->commit;
    
    return;
}

1;