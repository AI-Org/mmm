# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 11:16:14 2014

@author: ssoni
"""

import unittest

#from lib import Options

class TestCommandLineArguments(unittest.TestCase):
    def setUp(self):
        self.options = Options()

    def test_defaults_options_are_set(self):
        opts, args = self.options.parse()
        self.assertEquals(opts.example, 'example-value')
        print "done"