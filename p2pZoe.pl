use 5.016;
use warnings;
use strict;

use Data::Dumper;
use DateTime;
use DBI;
use threads;
use threads::shared;

use FTFCU::apwxvars;
use FTFCU::P2P;
use FTFCU::P2P::ZOE;

our $VERSION = 1.07;

say "p2pZoeExtract.pl" . "\nVersion: $VERSION";

say "Job started at " . DateTime->now( time_zone => 'America/Los_Angeles' );

run();

say "Job finished at " . DateTime->now( time_zone => 'America/Los_Angeles' );

sub run
{    
    my $apwx = FTFCU::apwxvars->new('OSIUPDATE','OSIUPDATE_PW');
    $apwx->reset_IO;
    
    say "I was passed the following args: " . Dumper( $apwx->{ARGV} );
    
    my %argsToCheck = (
        APWX_VARS   =>  {
            OSIUPDATE           =>  {   required => 1,  defined =>  1,                                  },
            OSIUPDATE_PW        =>  {   required => 1,  defined =>  1,                                  },
        },
        ARGV    =>  {
            MODE                =>  {   required => 1,  defined =>  1,  allow   =>  qr/^NEW|DELTA$/,    },
            TEST_YN             =>  {   required => 1,  defined =>  1,  allow   =>  qr/^Y|N$/           },
            MAX_THREADS         =>  {   required => 1,  defined =>  1,  allow   =>  qr/^\d+$/           },
            HOST                =>  {   required => 1,  defined =>  1,                                  },
            SID                 =>  {   required => 1,  defined =>  1,                                  },
            P2P_SERVER          =>  {   required => 1,  defined =>  1,                                  },
            P2P_SCHEMA          =>  {   required => 1,  defined =>  1,                                  },
            RPT_ONLY            =>  {   required => 1,  defined =>  1,  allow   =>  qr/^Y|N$/           },
            OUTPUT_FILE_PATH    =>  {   required => 1,  defined =>  1,                                  },
            OLD_ZOE_FILE        =>  {
                                        requred =>  $apwx->{ARGV}->{MODE} eq 'DELTA'
                                            ? 1
                                            : 0,
                                        defined =>  $apwx->{ARGV}->{MODE} eq 'DELTA'
                                            ? 1
                                            : 0,
                                                                                                        },
            NEW_ZOE_FILE       =>  {
                                        requred =>  $apwx->{ARGV}->{MODE} eq 'DELTA'
                                            ? 1
                                            : 0,
                                        defined =>  $apwx->{ARGV}->{MODE} eq 'DELTA'
                                            ? 1
                                            : 0,
                                                                                                        },
        },
        ENV =>  {
            
        },
    );
    
    $apwx->validate_args( \%argsToCheck );
    
    $apwx->{ARGV}->{OUTPUT_FILE_PATH} .= "\\"
        if $apwx->{ARGV}->{OUTPUT_FILE_PATH} !~ /\\$/;
    
    my $outputFilePath =
        $apwx->{ARGV}->{OUTPUT_FILE_PATH} . "AOEP2P01.FTF";

    open my $fhZoe, ">", "$outputFilePath"
        or die "Could not open file for output at $outputFilePath";
        
    say $fhZoe FTFCU::P2P::ZOE->buildCDERecord;
    
    my $seqnbr = 0;
    my $added = 0;
    my $changed = 0;
    my $acctHash = 0;
    
    say "ZOE file mode is " . $apwx->{ARGV}->{MODE};
    
    if ( $apwx->{ARGV}->{MODE} eq 'NEW' ){
        
        my @fileStatAry = stat $fhZoe;
        my @threadsAry;
        my @zoeData :shared;
        
        my $maxThreads = $apwx->{ARGV}->{MAX_THREADS};
        my $connectionNum = 0;
        
        say "Fetching ZOE records from DNA";
        
        foreach my $threadId ( 0 .. $maxThreads -1 ){
            my $apwx_t = $apwx;
            my $thread = threads->create( \&threadSub, ++$connectionNum, $apwx_t, $threadId, $maxThreads, \@zoeData, $apwx );
            push @threadsAry, $thread;
        }
        
        foreach ( @threadsAry ){
            $_->join();
        }
        
        say "Found " . scalar @zoeData . " ZOE records";
        
        my $headerRec = FTFCU::P2P::ZOE->buildHeaderRecord( { test => $apwx->{ARGV}->{RPT_ONLY}, fileType => 'LOAD' } );
        
        say $fhZoe $headerRec;
        
        say 'Printing ZOE file';        

        foreach ( @zoeData ){
            chomp;     
            my $line = $_;
            $line =~ s/\t+/ /g;
            my $detailFirst5;
            
            $detailFirst5 = join "|",
                '6',
                'A',
                $apwx->{ARGV}->{TEST_YN} eq 'Y'
                    ? '03'
                    : '01',
                'FTF',
                ++$seqnbr;
                
            $added++;
            
            my @lineAry = split /\|/, $line;
            $acctHash += $lineAry[3];
            my $acctStat = pop @lineAry;
            
            $line = join "|", $detailFirst5, @lineAry[0 .. 55 ];
            
            say $fhZoe $line;
        }
        
        my $trailerRec = FTFCU::P2P::ZOE->buildTrailerRecord(
            {
                recordCt    =>  scalar @zoeData + 2, # +2 is for header/trailer
                added       =>  $added,
                test        =>  $apwx->{ARGV}->{RPT_ONLY},
                fileType    =>  'LOAD',
                acctHash    =>  $acctHash,
            },
            \@zoeData,
            \@fileStatAry
        );
        
        print $fhZoe $trailerRec;
        
        $fhZoe->close;
    }
    elsif( $apwx->{ARGV}->{MODE} eq 'DELTA'){
        
        my $headerRec = FTFCU::P2P::ZOE->buildHeaderRecord( { test => $apwx->{ARGV}->{RPT_ONLY}, fileType => 'UPDT' } );
        
        say $fhZoe $headerRec;
        
        my @fileStatAry = stat $fhZoe;
        
        open my $fhZoeOld, '<', $apwx->{ARGV}->{OLD_ZOE_FILE}
            or die "Could not open old ZOE file at ";
        
        my ( $hashZoeOld, $junkAcctHash ) = FTFCU::P2P::ZOE->getZoeFileHash( $fhZoeOld );
        
        $fhZoeOld->close;
        
        open my $fhZoeNew, '<', $apwx->{ARGV}->{NEW_ZOE_FILE}
            or die "Could not open new ZOE file at ";
        
        my ( $hashZoeNew, $acctHash ) = FTFCU::P2P::ZOE->getZoeFileHash( $fhZoeNew );
        
        $fhZoeNew->close;
        
        say "Comparing New to Old";
        
        my $recCt;
        
        foreach my $k ( keys %{ $hashZoeNew } ){    
            
            my $detailFirst5;
    
            if ( $hashZoeOld->{$k} ){
                
                if ( $hashZoeNew->{$k} !~ /\Q$hashZoeOld->{$k}\E/ ){
                    # record has changed
                    $detailFirst5 = join "|",
                        '6',
                        'C',
                        $apwx->{ARGV}->{TEST_YN} eq 'Y'
                            ? '03'
                            : '01',
                        'FTF',
                        ++$seqnbr;
                    
                    $changed++;
                    
                    my @lineAry = split /\|/, $hashZoeNew->{$k};
                    
                    $acctHash += $lineAry[3];
                    $recCt++;
                        
                    say $fhZoe $detailFirst5 . '|' . $hashZoeNew->{$k};    
                }
                else{
                    # no change from previous file
                    # don't print to delta ZOE file
                }
            }
            else{
                # new record
                $detailFirst5 = join "|",
                    '6',
                    'A',
                    $apwx->{ARGV}->{TEST_YN} eq 'Y'
                        ? '03'
                        : '01',
                    'FTF',
                    ++$seqnbr;
                
                $added++;
                $recCt++;
                
                my @lineAry = split /\|/, $hashZoeNew->{$k};
                    
                $acctHash += $lineAry[3];
                
                say $fhZoe $detailFirst5 . '|' . $hashZoeNew->{$k};        
            }   
        }
        
        my $trailerRec = FTFCU::P2P::ZOE->buildTrailerRecord(
            {
                test        =>  $apwx->{ARGV}->{RPT_ONLY},
                fileType    =>  'UPDT',
                added       =>  $added,
                changed     =>  $changed,
                acctHash    =>  $acctHash,
                recordCt       =>  $recCt + 2, # +2 for header/trailer
            },
            undef,
            \@fileStatAry
        );
        
        print $fhZoe $trailerRec;
        
        $fhZoe->close;
    }
    else{
        # future use?    
    }

    return 1;
}

sub threadSub
{
    my ( $connectionNum, $apwx, $threadId, $maxThread, $zoeData, $apwxVars ) = @_;
    
    sleep $connectionNum;
    
    my %p2pArgs = (
        'zoe'       =>  1,
        'storeApwx' =>  'zoe',
        'getDnaDb'  =>  1,
        'getP2pDb'  =>  1,
        'host'      =>  $apwx->{ARGV}->{HOST},
        'sid'       =>  $apwx->{ARGV}->{SID},
        'user'      =>  $apwx->{APWX_VARS}->{OSIUPDATE},
        'pw'        =>  $apwx->{APWX_VARS}->{OSIUPDATE_PW},
        'p2pServer' =>  $apwx->{ARGV}->{P2P_SERVER},
        'p2pSchema' =>  $apwx->{ARGV}->{P2P_SCHEMA},
        'storeDbh'  =>  'zoe',
    );
    
    my $p2p = FTFCU::P2P->new( \%p2pArgs );
    
    say "Started thread: $threadId";
    
    $p2p->{zoe}->processZoeRecords( $maxThread, $threadId, $zoeData );
    $p2p->{zoe}->{dbh}->{dna}->disconnect();
    
    say "Finished thread: $threadId";
    
    return 1;
}

exit 0;