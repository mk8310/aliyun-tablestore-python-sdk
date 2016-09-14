#!/usr/bin/env python2.7
# -*- coding:utf-8 -*-

def GET_OBJ_DEFINE(input_obj):
    members = []
    sys_members = ["__module__", "__dict__", "__weakref__", "__doc__"]
    for i in input_obj.__dict__:
        if i not in sys_members:
            members.append(i)

    return members
