#! /usr/bin/python

import os
import sys
import subprocess
import time

WorkDir = "runproc"

def GetNextCmd(FNm, Cnt):
    InF = open(FNm, 'rt')
    LineV = InF.readlines()
    InF.close()

    LnCnt = 0
    for Line in LineV:
        # skip empty line or comments
        if len(Line.strip()) <= 0  or  Line[0] == "#":
            continue

        # skip lines already executed
        LnCnt += 1
        if LnCnt <= Cnt:
            continue

        yield Line.strip()

    yield ""

def RunCmd(ProcCnt, WorkDir, CmdStr, CmdId, MxProc):
    TmStr = time.strftime("%m-%d %H:%M:%S", time.localtime());
    print '__exec__ %s\t%d\t%d\t%s' % (TmStr, ProcCnt, CmdId, CmdStr.strip())
    BatFNm = os.path.join(WorkDir, 'xxxRunProc%d.sh' % ((CmdId%MxProc)+1))
    F = open(BatFNm, 'wt');
    F.write('#! /bin/sh\n')  
    F.write(CmdStr+'\n')
    F.write('exit 0\n')
    F.close()
    os.chmod(BatFNm, 0755)
    #FullCmdStr = 'C:\\WINDOWS\\system32\\cmd.exe /c "start "%d] %s %s" /wait %s"' % (CmdId, TmStr, CmdStr, BatFNm)
    FullCmdStr = '/bin/sh ' + BatFNm;
    Process = subprocess.Popen(FullCmdStr, shell=True)
    return Process

def WriteCnt(CntFn, ProcCnt):
    f = open(CntFn,"w")
    f.write("%d\n" % (ProcCnt))
    f.close()

if __name__ == '__main__':
    if len(sys.argv) < 2 :
        print 'Usage: %s [ -r ] <CommandFile> <RunMxProcs>' % (sys.argv[0])
        sys.exit(1)

    ArgInd = 1
    Recover = False
    if sys.argv[ArgInd] == "-r":
        Recover = True
        ArgInd += 1

    FNm = sys.argv[ArgInd]
    ArgInd += 1

    MxProc = 4;
    if len(sys.argv) > ArgInd:
        MxProc = int(sys.argv[ArgInd])

    CntFn = "rpRecover.txt"
    #print CntFn

    ProcCnt = 0
    if Recover  and  os.path.exists(CntFn):
        f = open(CntFn,"r")
        Line = f.readline()
        f.close()
        ProcCnt = int(Line)
    
    #print ProcCnt

    # create working directory
    try:
        os.makedirs(WorkDir)
    except OSError:
        if not os.path.isdir(WorkDir):
            raise

    ProcV = []
    # init
    GenCmd = GetNextCmd(FNm, ProcCnt)
    while len(ProcV) < MxProc :
        CmdStr = GenCmd.next()
        if len(CmdStr) == 0:
            sys.exit(1)
        ProcCnt += 1
        Tm0 = time.time()
        Proc = RunCmd(ProcCnt, WorkDir, CmdStr, len(ProcV)+1, MxProc)
        ProcV.append((Proc, ProcCnt, Tm0))
        WriteCnt(CntFn, ProcCnt)

    # run  
    Done = set()
    Tm1 = time.time()
    while len(Done) < MxProc:
        for Pid in range(0, len(ProcV)) :
            #print Pid, Done
            if Pid in Done:
                continue
            if ProcV[Pid][0].poll() != None :
                Tm4 = time.time()
                TmStr = time.strftime("%m-%d %H:%M:%S", time.localtime(Tm4));
                print '__done__ %s\t%d\t%d\t%.2f' % (
                        TmStr, ProcV[Pid][1], Pid+1, Tm4-ProcV[Pid][2])
                if len(Done) > 0:
                    Done.add(Pid)
                    continue
                CmdStr = GenCmd.next()
                if len(CmdStr) == 0:
                    Done.add(Pid)
                    continue
                ProcCnt += 1
                Tm3 = time.time()
                Proc = RunCmd(ProcCnt, WorkDir, CmdStr, Pid+1, MxProc)
                ProcV[Pid] = (Proc, ProcCnt, Tm3)
                WriteCnt(CntFn, ProcCnt)
        time.sleep(0.1)
    Tm2 = time.time()
    TmStr = time.strftime("%m-%d %H:%M:%S", time.localtime());
    print '__stat__ %s\t%d\t%d\t%.2f\n' % (TmStr, ProcCnt, MxProc, Tm2-Tm1)

