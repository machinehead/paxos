import paxos.essential as paxos
import logging

class Messenger (paxos.Messenger):
    proposers = {}
    acceptors = {}
    learners = {}
    queue = []

    processing = False

    def add_proposer(self, prop):
        self.proposers[prop.proposer_uid] = prop
        prop.messenger = self

    def add_acceptor(self, acc):
        self.acceptors[acc.acceptor_uid] = acc
        acc.messenger = self

    def add_learner(self, lrn):
        self.learners[lrn.learner_uid] = lrn
        lrn.messenger = self

    def send_prepare(self, proposal_id):
        '''
        Broadcasts a Prepare message to all Acceptors
        '''

        logging.info("PREPARE {}: bcast from proposer {}".format(proposal_id.number, proposal_id.uid))

        for acc in self.acceptors.itervalues():
            self.queue.append((acc.recv_prepare, (proposal_id.uid, proposal_id)))

        self.process_queue()

    def send_promise(self, proposer_uid, acceptor_uid, proposal_id, previous_id, accepted_value):
        '''
        Sends a Promise message to the specified Proposer
        '''

        logging.info("PROMISE {} from {}: to proposer {}, prev {}, accepted {}".format(proposal_id.number, acceptor_uid, proposer_uid, previous_id, accepted_value))

        self.queue.append((self.proposers[proposer_uid].recv_promise, (acceptor_uid, proposal_id, previous_id, accepted_value)))

        self.process_queue()

    def send_accept(self, proposal_id, proposal_value):
        '''
        Broadcasts an Accept! message to all Acceptors
        '''

        logging.info("ACCEPT! {}: bcast from proposer {}, value {}".format(proposal_id.number, proposal_id.uid, proposal_value))

        for acc in self.acceptors.itervalues():
            self.queue.append((acc.recv_accept_request, (proposal_id.uid, proposal_id, proposal_value)))

        self.process_queue()

    def send_accepted(self, proposal_id, acceptor_uid, accepted_value):
        '''
        Broadcasts an Accepted message to all Learners
        '''

        logging.info("ACCEPTED %r: acceptor %r, value %r", proposal_id.number, acceptor_uid, accepted_value)

        for lrn in self.learners.itervalues():
            self.queue.append((lrn.recv_accepted, (acceptor_uid, proposal_id, accepted_value)))

        self.process_queue()

    def on_resolution(self, proposal_id, value):
        '''
        Called when a resolution is reached
        '''

        logging.info("RESOLUTION %r: %r", proposal_id.number, value)

    def process_queue(self):
        if self.processing:
            return

        self.processing = True

        while self.queue:
            msg = self.queue.pop(0)
            msg[0](*msg[1])

        self.processing = False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    proposer_uid_base = 0
    acceptor_uid_base = 0
    learner_uid_base = 0
    quorum = 2

    m = Messenger()

    p = paxos.Proposer()
    p.proposer_uid = 1
    p.quorum_size = quorum
    m.add_proposer(p)

    for i in xrange((quorum - 1) * 2 + 1):
        a = paxos.Acceptor()
        acceptor_uid_base += 1
        a.acceptor_uid = acceptor_uid_base
        m.add_acceptor(a)

    for i in xrange((quorum - 1) * 2 + 1):
        l = paxos.Learner()
        learner_uid_base += 1
        l.learner_uid = learner_uid_base
        l.quorum_size = quorum
        m.add_learner(l)

    p.set_proposal(1)
    p.prepare()
    p.set_proposal(2)
    p.prepare()
