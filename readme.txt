CS4288 Project 4 Readme
sangam.subedi@vanderbilt.edu smriti.timalsina@vanderbilt.edu shruti.timalsina@vanderbilt.edu

Generative AI Disclosure

For each item, mark with [x] if you used generative AI for that
category.  Then, describe how you used it in one paragraph.

[X] Conceptual understanding.

    Did you use an LLM to help you understand a concept described in the
    lectures or assignment spec?  If so, describe how you used it.

    We asked it to explain what MapReduce and Hadoop/Madoop are and how
    they fit this project. We also asked about specific details, like
    why recv() reads 4096 bytes at a time and how TCP differs from UDP.

[X] High-level design.

    Did you use an LLM to help you plan out architecture for your
    submission?  Like directories, which files go where, and what
    functions to implement?

    It helped us plan the threads (TCP listener, UDP heartbeat listener,
    job runner, and heartbeat checker) and the shared networking module in
    mapreduce/utils/network.py. We went through it one spec section at a
    time and talked through each piece before writing it.

[X] Implementation.

    Did you use an LLM to help you implement functionality described in
    the spec?

    Yes, it wrote most of the Manager, Worker, and bin/mapreduce code with
    us, step by step. It explained each line, and we
    reviewed and committed the code after each step.

[X] Test cases.

    Did you use an LLM to write test cases?

    We didn't add new pytest tests. It wrote small throwaway scripts with
    fake Workers to check things the public tests don't cover, like
    Workers waiting for a free slot and getting shutdown messages.

[X] Testing.

    Did you use an LLM to evaluate implementation against test cases?

    It ran the public tests and the style checkers after each step, and
    ran a full word count job and diffed the output against correct.txt.
    It also killed a Worker in the middle of a job to check fault tolerance.

[ ] End-to-end use.

    Did you just paste the spec into the LLM?


We may allot up to 3% bonus credit to students who provide their entire
chat history/transcripts.



If you did not use any generative AI for this assignment, please mark
below:

[ ] On my honor as a student, I have not used generative AI or LLMs in
any way to complete any part of this assignment.
