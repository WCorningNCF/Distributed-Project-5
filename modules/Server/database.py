# -----------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# -----------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 24 July 2013
#
# Copyright 2012 Linkoping University
# -----------------------------------------------------------------------------

# Wiley Corning 8/31/15

"""Implementation of a simple database class."""

import random


class Database(object):

    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        
        db = open(self.db_file,'r')
        contents = db.read()
        db.close()
        self.fortunes = str.split(contents,'\n%\n')
        self.fortunes=self.fortunes[:-1] # Remove empty fortune at end
        
    def read(self):
        """Read a random location in the database."""
        return(self.fortunes[self.rand.randint(0,len(self.fortunes)-1)])

    def write(self, fortune):
        """Write a new fortune to the database."""
        # Add to stored list
        self.fortunes.append(fortune)
        
        # Append to file
        db = open(self.db_file,'a')
        db.write(fortune+'\n%\n')
        db.close()