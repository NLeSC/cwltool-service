import connexion
from flask import Flask, Response, request, redirect
import os
import subprocess
import tempfile
import json
import yaml
import urlparse
import signal
import threading
import time
import copy


jobs_lock = threading.Lock()
jobs = []


class Job(threading.Thread):
    def __init__(self, jobid, path, inputobj, name=''):
        super(Job, self).__init__()
        self.jobid = jobid
        self.path = path
        self.inputobj = inputobj
        self.name = name
        self.updatelock = threading.Lock()
        self.begin()

    def begin(self):
        loghandle, self.logname = tempfile.mkstemp()
        with self.updatelock:
            self.outdir = tempfile.mkdtemp()
            self.status = {
                "id": "%sjobs/%s" % (request.url_root, self.jobid),
                "log": "%sjobs/%s/log" % (request.url_root, self.jobid),
                "workflow": self.path,
                "state": "Running",
                "input": self.inputobj,
                "output": None,
                "name": self.name}
            try:
                self.proc = subprocess.Popen(["cwl-runner", self.path, "-"],
                                             stdin=subprocess.PIPE,
                                             stdout=subprocess.PIPE,
                                             stderr=loghandle,
                                             close_fds=True,
                                             cwd=self.outdir)
            except OSError:
                self.status["state"] = "SystemError"



    def run(self):
        self.stdoutdata, self.stderrdata = self.proc.communicate(json.dumps(self.inputobj))
        if self.proc.returncode == 0:
            outobj = yaml.load(self.stdoutdata)
            with self.updatelock:
                self.status["state"] = "Success"
                self.status["output"] = outobj
        else:
            with self.updatelock:
                self.status["state"] = "PermanentFailure"

    def getstatus(self):
        with self.updatelock:
            return self.status.copy()

    def cancel(self):
        if self.status["state"] == "Running":
            self.proc.send_signal(signal.SIGQUIT)
            with self.updatelock:
                self.status["state"] = "Cancelled"

    def pause(self):
        if self.status["state"] == "Running":
            self.proc.send_signal(signal.SIGTSTP)
            with self.updatelock:
                self.status["state"] = "Paused"

    def resume(self):
        if self.status["state"] == "Paused":
            self.proc.send_signal(signal.SIGCONT)
            with self.updatelock:
                self.status["state"] = "Running"



def postJob(body):
    path = body['workflow']
    inputobj = body.get('input', {})
    name = body.get('name', '')
    with jobs_lock:
        jobid = len(jobs)
        job = Job(str(jobid), path, inputobj, name)
        job.start()
        jobs.append(job)
    return redirect("/jobs/%i" % jobid, code=303)



def getJobById(jobId):
    with jobs_lock:
        try:
            job = jobs[int(jobId)]
        except IndexError:
            return 'Not Found', 404
    return job.getstatus()


def logspooler(job):
    with open(job.logname, "r") as f:
        while True:
            r = f.read(4096)
            if r:
                yield r
            else:
                with job.updatelock:
                    if job.status["state"] != "Running":
                        break
                time.sleep(1)


def getJobLogById(jobId):
    with jobs_lock:
        try:
            job = jobs[int(jobId)]
        except IndexError:
            return 'Not Found', 404
    return Response(logspooler(job))


def getJobs():
    with jobs_lock:
        jobscopy = copy.copy(jobs)
    def spool(jc):
        yield "["
        first = True
        for j in jc:
            if first:
                yield json.dumps(j.getstatus(), indent=4)
                first = False
            else:
                yield ", "  + json.dumps(j.getstatus(), indent=4)
        yield "]"
    return Response(spool(jobscopy))


def deleteJobById(jobId):
    with jobs_lock:
        try:
            job = jobs[int(jobId)]
        except IndexError:
            return 'Not Found', 404
    status = job.getstatus()
    if status in ('Running', 'Waiting'):
        job.cancel()
    del jobs[int(jobId)]


def cancelJobById(jobId):
    with jobs_lock:
        try:
            job = jobs[int(jobId)]
        except IndexError:
            return 'Not Found', 404
    job.cancel()
    return job.getstatus()


app = Flask(__name__)
app = connexion.App(__name__, specification_dir='workflow-execution-schemas/src/main/resources/swagger/')
app.add_api('ga4gh-workflow-execution.yaml', resolver=lambda operation_id: globals()[operation_id])

if __name__ == "__main__":
    app.debug = True
    app.run()
