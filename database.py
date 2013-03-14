##TODO: DB reworked from MySQL to sqlite, many bugs here

from multiprocessing import Process, Queue
import sqlite3 as sqlite
import os
import logger

class database:

        _stop = 'stop'

        def __init__(self, options):
                self.options = options
                self.queue = Queue()
                self.proc = Process(target=self.loop)
                self.conn = None
                self.logger = logger.logger(self)
                self.cursor = None

        def start(self):
                self.connect()
                self.proc.start()

        def stop(self):
                self.push(self._stop)
                self.proc.join()

        def push(self, command):
                self.queue.put(command, block=False)

        def connect(self):
                if os.path.isfile(self.options.db):
                        self.logger.info("Using exiting database: %s" % (self.options.db))
                else:
                        self.logger.info("New database created: %s" % (self.options.db))

                try:
                        self.conn = sqlite.connect(self.options.db)
                        self.cursor = self.conn.cursor()
                except sqlite.Error, e:
                        self.logger.error("Could not connect to database: %s" % (e[1]))
                        exit(1)
        
        def loop(self):
                while True:
                        command = self.queue.get()
                        if command == self._stop:
                                break
                        self.process(command)
                        ## TODO: maybe commit later?
                        #self.conn.commit()

        def process(self, statement):

                return
                ## tmp db remove 
                command, args = statement
                try:
                        self.cursor.execute(command, args)
                except (AttributeError, sqlite.OperationalError, sqlite.ProgrammingError), e:
                        self.logger.warn("Fail to process SQL statement: (%s, %s): %s" % (command, args, e[0]))

        # TODO: remove this shit
        def escape(self, string):
                return string

