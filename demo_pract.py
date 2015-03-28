import paxos.practical as paxos
import demo_essent
import logging

class Messenger (demo_essent.Messenger, paxos.Messenger):
    def send_prepare_nack(self, to_uid, proposal_id, promised_id):
        '''
        Sends a Prepare Nack message for the proposal to the specified node
        '''

        logging.error("PREPARE NACK %r: to proposer %r, promised %r", proposal_id.number, to_uid, promised_id)

    def send_accept_nack(self, to_uid, proposal_id, promised_id):
        '''
        Sends a Accept! Nack message for the proposal to the specified node
        '''

        logging.error("ACCEPT! NACK %r: to proposer %r, promised %r", proposal_id.number, to_uid, promised_id)

class AutoSaveMixin(object):

    auto_save = True
    
    def recv_prepare(self, from_uid, proposal_id):
        super(AutoSaveMixin, self).recv_prepare(from_uid, proposal_id)
        if self.persistance_required and self.auto_save:
            self.persisted()

    def recv_accept_request(self, from_uid, proposal_id, value):
        super(AutoSaveMixin, self).recv_accept_request(from_uid, proposal_id, value)
        if self.persistance_required and self.auto_save:
            self.persisted()

class Node (AutoSaveMixin, paxos.Node):
    @property
    def acceptor_uid(self):
        return self.node_uid

    @property
    def learner_uid(self):
        return self.node_uid

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    next_node_id = 1
    quorum = 2

    m = Messenger()
    nodes = []

    for i in xrange((quorum - 1)*2 + 1):
        node = Node(m, next_node_id, quorum)
        next_node_id += 1
        m.add_proposer(node)
        m.add_acceptor(node)
        m.add_learner(node)
        nodes.append(node)

    logging.debug("Setting proposal 1 to node 1")
    nodes[0].set_proposal(1)
    logging.debug("Done setting proposal 1 to node 1")
    logging.debug("Preparing proposal from node 1")
    nodes[0].prepare()
    logging.debug("Done preparing proposal from node 1")
    logging.debug("Setting proposal 1 to node 1")
    nodes[0].set_proposal(2)
    logging.debug("Done setting proposal 1 to node 1")
    logging.debug("Preparing proposal from node 1")
    nodes[0].prepare()
    logging.debug("Done preparing proposal from node 1")
