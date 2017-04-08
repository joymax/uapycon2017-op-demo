#!/bin/python

import sys
import random


def insert_str(string, str_to_insert, index):
    return string[:index] + str_to_insert + string[index:]


class Channel(object):
    def __init__(self):
        self.sites = {}

    def register(self, site_id, site):
        self.sites[site_id] = site
        print "[CHANNEL] Site %s registered" % site_id

    def broadcast(self, c_site_id, op):
        items = self.sites.items()
        random.shuffle(items)
        for site_id, site in items:
            if site_id != c_site_id:
                site.receive(*op)


class Site(object):
    def __init__(self, n, conn):
        self.n = n
        self.state = {}
        self.queue = []
        self.log = []
        self.priority = 0
        self.conn = conn
        self.apply_commands = []

    def generate(self, op):
        # op consist from array of two elems: [op_id, index]
        payload = [self.n, self.state, op, self.priority]
        self.queue.append(payload)
        self.priority += 1
        self.conn.broadcast(self.n, payload)

    def receive(self, site_id, state, op, priority):
        self.queue.append([site_id, state, op, priority])

    def transform(self, u, v, op1, op2, p1, p2):
        pos1, pos2 = op1[1], op2[1]
        if p1 < p2:
            return [op1[0], pos2]
        elif p1 > p2:
            return [op1[0], pos1 + pos2]
        else:
            if op1[0] == op2[0]:
                return None
            else:
                if p1 > p2:
                    return [op1[0], pos1 + pos2]
                else:
                    return op1


    def execute(self):
        """dOPT implementation"""
        print " [EXEC] Site %d" % self.n
        for site_id, state, op, priority in self.queue:
            # remote less than local, transformation required
            if self.state.get(site_id, 0) > state.get(site_id, 0):
                entry = self.log.pop()
                while entry and op:
                    k_site_id, k_state, k_op, k_priority = entry
                    # we don't need to transform operations from same site
                    if k_site_id == site_id:
                        entry = self.log.pop() if self.log else []
                        continue

                    # if local state for site less than remote we need to converge
                    if state.get(k_site_id, 0) <= k_state.get(k_site_id, 0):
                        u = op[1]
                        v = k_op[1]
                        print "  [TRANSFORM] (site %d, %d) ==> %r %r" % (site_id, k_site_id, op, k_op)
                        op = self.transform(u, v, op, k_op, priority, k_priority)
                        print "[TRANSFORM] [RESULT] %r" % op
                    entry = self.log.pop() if self.log else []

            self.apply_commands.append(op)
            self.log.append([site_id, state, op, priority])
            if not site_id in self.state:
                self.state[site_id] = 0
            self.state[site_id] += 1

        self.q = []

    def show_state(self):
        print "[FINAL STATE] Site #%d:" % self.n
        result = ""
        for text, position in self.apply_commands:
            result = insert_str(result, text, position)
        print
        print "             %s" % result
        print



def demo(args):
    conn = Channel()
    site1 = Site(1, conn)
    site2 = Site(2, conn)
    site3 = Site(3, conn)
    sites = [site1, site2, site3]
    conn.register(1, site1)
    conn.register(2, site2)
    conn.register(3, site3)

    site1.generate(["T1", 0]) # T1
    site2.generate(["T3", 2]) # T1T3
    site2.generate(["T4", 2]) # T1T4T3
    site3.generate(["T2", 2]) # T1T2T4T3
    site1.generate(["T0", 0]) # T0T1T2T4T3

    [s.execute() for s in sites]
    [s.show_state() for s in sites]



COMMANDS = {"demo": demo}

def main(args):
    if not args or args[0] not in COMMANDS:
        raise ValueError("\n\n   ERROR: Command not found. Commands are: %s\n" % ", ".join(COMMANDS))

    COMMANDS[args[0]](args[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
