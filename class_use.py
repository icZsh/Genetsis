#!/usr/bin/env python3
# -*- coding: utf-8 -*-

class Student(object):
    def __init__(self, name, score,gender):
        self.__name = name
        self.__score = score
        self.__gender = gender

    def get_gender(self):
        return self.__gender

    def get_name(self):
        return self.__name

    def get_score(self):
        return self.__score

    def set_name(self,name):
        self.__name=name

    def set_score(self,score):
        self.__score=score

    def set_gender(self,gender):
        if gender.capitalize()=='Male':
            self.__gender=gender
        elif gender.capitalize()=='Female':
            self.__gender=gender
        else:
            raise ValueError('bad gender')


    def print_info(self):
        print('%s(%s):%s' % (self.get_name(), self.get_gender(),self.get_score()))


bart = Student('Bart Simpson', 59,'female')
bart.set_name("Genius")
bart.set_gender("male")
bart.set_score(100)
bart.print_info()
