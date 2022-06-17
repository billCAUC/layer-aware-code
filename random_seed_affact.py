# -*- coding: utf-8 -*-
import numpy as np
import random
import pickle
import pathlib
import networkx as nx
import collections
import queue
import pseudoflow
import utils2 as utils
import time
import copy
# import matplotlib.pyplot as plt
import statsmodels as sm


class Trace:
    def __init__(self):
        self.trace_file = "trace.pkl"
        self.task_list = []
        self.allContainer = []
        self.allLayer = []
        self.image_stats = dict()
        self.image_name_list = []
        '''
        layer_stats key 为layer名字（hash码） value为list
        value的list当中包含[star数量,下载次数,被多少个image（只统计了我们下载的image）包含,大小, 编号,具体参与到的image名字]
        'sha256:8d380c957e3e85d308c5520a5fa0a29687cf3bb956cd141b5fc14c78cf9dfed1': ['110', '159234', '1', '639052673',10,['swift']]
        '''
        self.layer_stats = dict()

        with open("pull/image_stats.csv", "r") as f:
            cnt = 0
            for lines in f.readlines():
                image_item = lines[:-1].split(",")
                self.image_stats[image_item[0]] = image_item[1:]
                self.image_stats[image_item[0]].append(cnt)
                self.allContainer.append(self.image_stats[image_item[0]])
                self.image_name_list.append(image_item[0])
                cnt += 1
                #print(self.image_stats[image_item[0]])
        with open("pull/layer_stats.csv", "r") as f:
            cnt = 0
            for lines in f.readlines():
                layer_item = lines[:-1].split(",")
                if int(layer_item[1:-1][3]) > 0:
                    self.layer_stats[layer_item[0]] = layer_item[1:-1]
                    self.layer_stats[layer_item[0]].append(cnt)
                    self.layer_stats[layer_item[0]].append(
                        [i[8:] for i in layer_item[-1].split("|")])
                    for i in layer_item[-1].split("|"):
                        if len(self.image_stats[i[8:]]) == 5:
                            self.image_stats[i[8:]].append([])
                        self.image_stats[i[8:]][5].append(layer_item[0])
                    self.allLayer.append(self.layer_stats[layer_item[0]])
                    #print(self.layer_stats[layer_item[0]])
                    self.layer_stats[layer_item[0]][3] = int(self.layer_stats[layer_item[0]][3])
                    cnt += 1
        '''
        image_name_list 包含所有image名字的list
        ['node-env', 'ubuntu-upstart', 'iojs', 'ruby-env', 'perl',...]
        '''

class Machine:
    def __init__(self):
        self.layer_list = []                          ## layer name，in sequence 
        self.container_list = []                      ## container name, in sequence
        self.layer_download_finish_time = {}          ## 
        self.container_download_finish_time = {}
        self.download_finish_time = 0
        self.current_container_num = 0
        self.container_limit = 50
        self.container_name_num = {}
        self.storage_limit =20*1024*1024*1024
        self.current_storage = 0
        self.bandwidth = (10/8)*(1024*1024)

def get_download_finish_time(_machine,layer_stats,image_layer,alpha):
    already_download = True
    total_download_size = 0
    already_finish_time = 0
    for _layer_name in image_layer:
        if _layer_name not in _machine.layer_list:
            already_download = False
            total_download_size += layer_stats[_layer_name]
        else:
            if already_finish_time < _machine.layer_download_finish_time[_layer_name]:
                already_finish_time = _machine.layer_download_finish_time[_layer_name]
    if already_download:
        return already_finish_time
    else:
        return (_machine.download_finish_time)*alpha+(total_download_size/_machine.bandwidth)*(1-alpha)

def compare_least_container_size_and_storage_size(machine_list,_machine_index,layer_stats,container_stats,container_counter):
    min_container_size = 10000000000000000
    min_container_name = 0
    for container_name in container_counter:
        if(container_counter[container_name] >0):
            current_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[container_name] if _layer_name not in machine_list[_machine_index].layer_list])
            if(min_container_size>=current_container_size):
                min_container_size = current_container_size
                min_container_name = container_name
    if(min_container_size <= machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage):
#        print(min_container_size,machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage)
        return False
    else:
        return True
def get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter):
    ### index是machine的index
    min_container_name = ""
    min_machine = 0
    min_time = 10000000000000000
    for _container_name in time_vector:
        for index, j in enumerate(time_vector[_container_name]):
            divide_ratio = min(machine_list[index].container_limit - machine_list[index].current_container_num,container_counter[_container_name])
            current_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[index].layer_list])
#            current_container_download_time = current_container_size/machine_list[index].bandwidth
            if(current_container_size<= machine_list[index].storage_limit - machine_list[index].current_storage):
                if j/divide_ratio < min_time:
                    min_container_name = _container_name
                    min_machine = index
                    min_time = j/divide_ratio
    return min_container_name, min_machine

def get_least_download_time_pair2(machine_list,time_vector,layer_stats,container_stats,container_counter):
    min_container_name = ""
    min_machine = 0
    min_time = 10000000000000000
    for _container_name in time_vector:
        for index, j in enumerate(time_vector[_container_name]):
            current_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[index].layer_list])            
            if(current_container_size <=current_container_size<= machine_list[index].storage_limit - machine_list[index].current_storage):
                if j[1]-j[0] < min_time and machine_list[index].current_container_num<machine_list[index].container_limit:
                    min_container_name = _container_name
                    min_machine = index
                    min_time = j[1]-j[0]
    return min_container_name, min_machine
def get_least_download_time_pair3(machine_list,time_vector,layer_stats,container_stats,container_counter,alpha):
    ### index是machine的index
    min_container_name = ""
    min_machine = 0
    min_time = 10000000000000000
    for _container_name in time_vector:
        for index, j in enumerate(time_vector[_container_name]):
            divide_ratio = min(machine_list[index].container_limit - machine_list[index].current_container_num,container_counter[_container_name])
            current_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[index].layer_list])
#            current_container_download_time = current_container_size/machine_list[index].bandwidth
            if(current_container_size<= machine_list[index].storage_limit - machine_list[index].current_storage):
                if j/divide_ratio < min_time:
                    min_container_name = _container_name
                    min_machine = index
                    min_time = j/divide_ratio
    return min_container_name, min_machine
    
def schedule_container_random(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_unique_counter = [i for i in range(machine_number)]            ####
    np.random.seed(seed)
    visit_sequence = np.random.permutation(len(container_list_unique))
    container_counter_copy = copy.deepcopy(container_counter)
    C_time_list = dict()
    for i in visit_sequence:
        _container_name = container_list_unique[i]
        machine_queue = np.random.permutation(len(machine_unique_counter)).tolist()
        while(len(machine_queue) != 0):
            _machine_index = machine_queue.pop()
            add_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])
            if(add_container_size <= machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage and machine_list[_machine_index].container_limit > machine_list[_machine_index].current_container_num):
                machine_list[_machine_index].current_storage += add_container_size
                for _layer_name in  container_stats[_container_name]:
                    if _layer_name not in machine_list[_machine_index].layer_list:
                        machine_list[_machine_index].layer_list.append(_layer_name)
                        machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                        machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time 
                machine_list[_machine_index].container_list.append(_container_name)
                if(machine_list[_machine_index].container_limit-machine_list[_machine_index].current_container_num>=container_counter_copy[_container_name]):
                    machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name]
                    C_time_list[_container_name] = container_counter_copy[_container_name]*max([machine_list[_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                    machine_list[_machine_index].container_download_finish_time[_container_name] = C_time_list[_container_name]
                    machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
                    break
                else:                    
                    machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
                    container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)
                    C_time_list[_container_name] = (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)* max([machine_list[_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                    machine_list[_machine_index].container_download_finish_time[_container_name] = C_time_list[_container_name]
                    machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit 

                                                        
#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list]

#    print(complete_time_list)  
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    print([machine.storage_limit for machine in machine_list])
#    print([machine.container_list for machine in machine_list])
#    print([machine.current_container_num for machine in machine_list])  
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list[_machine_index].layer_list])  for _machine_index in range(machine_number)]
    return layer_size_per_machine,total_time,complete_time_list
    
    # sum(c_machine_list)                
                
        

def schedule_container_in_sequence(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)] 
    machine_list_copy = []                                                ###
    visit_sequence = np.random.permutation(len(container_list_unique))
    container_counter_copy = copy.deepcopy(container_counter)    
    C_time_list = dict()
    for i in visit_sequence:        
        _container_name = container_list_unique[i]
        _min_download_finish_time  = 1000000000000000000000000
        _min_machine_index = -1
        for _machine_index,_machine in enumerate(machine_list):                                                         ###                                                   ###
            #download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]                 ###
            #_machine_index = np.argmin(np.array(download_finish_time))            
            add_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in _machine.layer_list])            
            if(add_container_size<= _machine.storage_limit - _machine.current_storage):
                _download_finish_time = get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha)
                if(_download_finish_time<_min_download_finish_time):
                    _min_machine_index = _machine_index
                    _min_download_finish_time = _download_finish_time
       #                print("_machine_index",_machine_index,"_container_name",_container_name)
        if(_min_machine_index != -1):
            for _layer_name in  container_stats[_container_name]:
                if _layer_name not in machine_list[_min_machine_index].layer_list:
                    machine_list[_min_machine_index].layer_list.append(_layer_name)
                    machine_list[_min_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_min_machine_index].bandwidth
                    machine_list[_min_machine_index].layer_download_finish_time[_layer_name] = machine_list[_min_machine_index].download_finish_time
            machine_list[_min_machine_index].container_list.append(_container_name)
            machine_list[_min_machine_index].current_storage += add_container_size
            
            if machine_list[_min_machine_index].container_limit-machine_list[_min_machine_index].current_container_num>=container_counter_copy[_container_name]:
                machine_list[_min_machine_index].current_container_num = machine_list[_min_machine_index].current_container_num + container_counter_copy[_container_name]
                C_time_list[_container_name] = container_counter_copy[_container_name]*max([machine_list[_min_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                machine_list[_min_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
                if machine_list[_min_machine_index].container_limit==machine_list[_min_machine_index].current_container_num:
                    machine_list_copy.append(machine_list.pop(_min_machine_index))
            else:    
                machine_list[_min_machine_index].container_name_num[_container_name] = machine_list[_min_machine_index].container_limit - machine_list[_min_machine_index].current_container_num
                container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_min_machine_index].container_limit - machine_list[_min_machine_index].current_container_num)
                C_time_list[_container_name] = (machine_list[_min_machine_index].container_limit - machine_list[_min_machine_index].current_container_num)* max([machine_list[_min_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                machine_list[_min_machine_index].current_container_num = machine_list[_min_machine_index].container_limit
                if machine_list[_min_machine_index].container_limit==machine_list[_min_machine_index].current_container_num:
                    machine_list_copy.append(machine_list.pop(_min_machine_index)) 

        if(len(machine_list) == 0):
            break;
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))    
    print(sum(list(C_time_list.values())))
    
    
    
#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
#    print([machine.storage_limit for machine in machine_list_copy])
    print([machine.container_name_num for machine in machine_list_copy])
#    print([machine.current_container_num for machine in machine_list_copy])     
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    
    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
#    print(complete_time_tuple)
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
#    print(complete_time_list)
    return layer_size_per_machine,total_time,complete_time_list

def schedule_container_machine_greedy1(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
#     C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
#    print([machine.storage_limit for machine in machine_list_copy])
    print([machine.container_name_num for machine in machine_list_copy])
#    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]

    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
#    print(complete_time_tuple)
#    print("sdfsdfsdfsdfsdfsdfsdfsdf")
#    quit()
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
#    print(complete_time_list)
    return layer_size_per_machine,total_time,complete_time_list

def schedule_container_machine_greedy2(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
#     C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_machine_greedy3(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
#    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
    C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_machine_greedy4(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
#    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
#     C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
    C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_machine_greedy5(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
#    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
#     C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
    C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_machine_greedy6(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []    
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    container_counter_copy = copy.deepcopy(container_counter)
    remained_container_list_unique = container_list_unique.copy()
    while(len(remained_container_list_unique) > 0):
        if(len(machine_list) == 0):
            break
        time_vector = dict()
        for i in visit_sequence:
            _container_name = container_list_unique[i]
            download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
            time_vector[_container_name] = download_finish_time
        _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
            
        if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
            visit_sequence.remove(container_list_unique.index(_container_name))
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            remained_container_list_unique.remove(_container_name)
        else:            
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
 
            
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
                machine_list[_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_machine_index].bandwidth
                machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
        machine_list[_machine_index].container_list.append(_container_name)
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_copy.append(machine_list.pop(_machine_index))
            
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))

#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
#    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
#    
#
#     C_time_list = [utils.exact_result(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_counter,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_greedy(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#
#     C_time_list = [utils.get_single_machine_total_weighted_time_by_random(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
#    
    C_time_list = [utils.get_single_machine_total_weighted_time_by_SPT(_machine.bandwidth,_machine.container_name_num,container_stats,layer_stats) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_machine_diff_greedy(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(1)
    total_weight_time_dict = dict()
    machine_list = [Machine() for i in range(machine_number)]
    visit_sequence = np.random.permutation(len(container_list_unique)).tolist()
    time_vector = dict()
    total_weighted_finish_time_before_allocation = [0 for _machine in machine_list]
    machine_list_full_flag = [1 for _machine in machine_list]
    container_counter_copy = copy.deepcopy(container_counter)
    for _ in range(len(container_counter_copy)):         # 对于每个container
#        print(_)
        for i in visit_sequence:
            _container_name = container_list_unique[i]   # 随机每个container
            add_machine_list = [[] for _machine in machine_list]
            for _machine_index,_machine in enumerate(machine_list):
                add_machine_list[_machine_index].extend(_machine.container_list)
                add_machine_list[_machine_index].append(_container_name)
            
            total_weighted_finish_time = []
            for _machine_index,_machinelist in enumerate(add_machine_list):
                if tuple(_machinelist) in total_weight_time_dict:
                    total_weighted_finish_time.append(total_weight_time_dict[tuple(_machinelist)])
                else:
                    total_weight_time_dict[tuple(_machinelist)] = utils.get_single_machine_total_weighted_time2(machine_list[_machine_index].bandwidth,_machinelist,container_stats,layer_stats,container_counter_copy)
                    total_weighted_finish_time.append(total_weight_time_dict[tuple(_machinelist)])
                
            if _container_name not in time_vector:
                time_vector[_container_name] = [(0,_time,_time) for _time in total_weighted_finish_time]
            else:
                time_vector[_container_name] = [(total_weighted_finish_time_before_allocation[_index],total_weighted_finish_time[_index],total_weighted_finish_time[_index]-total_weighted_finish_time_before_allocation[_index]) for _index in range(machine_number)]
                
        _container_name, _machine_index = get_least_download_time_pair2(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
        if(_container_name == ""):
            break
        else:
            machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
        machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
        if machine_list[_machine_index].container_limit-machine_list[_machine_index].current_container_num>=container_counter_copy[_container_name]:
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name]
            machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            total_weighted_finish_time_before_allocation[_machine_index] = time_vector[_container_name][_machine_index][1]
            visit_sequence.remove(container_list_unique.index(_container_name)) 
            del time_vector[_container_name]
        else:
#            print(_container_name)
#            print(container_counter[_container_name])
#            print(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)
            container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)
            machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
            machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
            total_weighted_finish_time_before_allocation[_machine_index] = time_vector[_container_name][_machine_index][1]

        
        for _layer_name in container_stats[_container_name]:
            if _layer_name not in machine_list[_machine_index].layer_list:
                machine_list[_machine_index].layer_list.append(_layer_name)
        machine_list[_machine_index].container_list.append(_container_name)
        
        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
            machine_list_full_flag[_machine_index] = 0         
        if(sum(machine_list_full_flag) == 0):
            break 
        
    c_machine_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
    print(sum(c_machine_list))
    print([machine.container_list for machine in machine_list])
    print([machine.current_container_num for machine in machine_list])  
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list[_machine_index].layer_list])  for _machine_index in range(machine_number)]
    return layer_size_per_machine,sum(c_machine_list)

def schedule_container_group_machine_greedy(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = [] 
    container_counter_copy =  copy.deepcopy(container_counter)     
    for subgroup in group_list:
        for i in range(len(subgroup)):  #这个顺序存在随机性
            _container_name = subgroup[i]
            while(1):
                if(len(machine_list) == 0):                                   ###
                    break;                                                    ###                
                download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
                _machine_index = np.argmin(np.array(download_finish_time))
                add_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])            
                if(add_container_size<= machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage):
                    machine_list[_machine_index].current_storage += add_container_size
                    machine_list[_machine_index].container_list.append(_container_name)
                    for _layer_name in  container_stats[_container_name]:
                        if _layer_name not in machine_list[_machine_index].layer_list:
                            machine_list[_machine_index].layer_list.append(_layer_name)
                            machine_list[_machine_index].download_finish_time += int(layer_stats[_layer_name]/machine_list[_machine_index].bandwidth)
                            machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
                            
                    if machine_list[_machine_index].container_limit-machine_list[_machine_index].current_container_num>=container_counter_copy[_container_name]:
                        machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name]
                        machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
                        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
                            machine_list_copy.append(machine_list.pop(_machine_index))
                        break
                    else:
                        container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)
                        machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
                        machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit                    
                        if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
                            machine_list_copy.append(machine_list.pop(_machine_index))  
                else:
                    break
            if(len(machine_list) == 0):
                break;
        if(len(machine_list) == 0):
            break; 
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))      
            
    # C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])  
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_group_greedy_machine_greedy(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    machine_list_copy = []                                                    ###
    container_counter_copy = copy.deepcopy(container_counter)
#    container_counter_copy = container_counter                                ###        
    for subgroup in group_list:
        visit_sequence = np.random.permutation(len(subgroup)).tolist()
        for _ in range(len(subgroup)):
            if(len(machine_list) == 0):
                break            
            time_vector = dict()
            for i in visit_sequence:
                _container_name = subgroup[i]
                download_finish_time = [get_download_finish_time(_machine,layer_stats,container_stats[_container_name],alpha) for _machine in machine_list]
                time_vector[_container_name] = download_finish_time
            _container_name, _machine_index = get_least_download_time_pair(machine_list,time_vector,layer_stats,container_stats,container_counter_copy)
            if(_container_name == ""):
                break
            else:
                machine_list[_machine_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])
            if(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num >= container_counter_copy[_container_name]):
                visit_sequence.remove(subgroup.index(_container_name))
                machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[_container_name] 
                machine_list[_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            else:
                container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)            
                machine_list[_machine_index].container_name_num[_container_name] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
                machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
            machine_list[_machine_index].container_list.append(_container_name)
            for _layer_name in container_stats[_container_name]:
                if _layer_name not in machine_list[_machine_index].layer_list:
                    machine_list[_machine_index].layer_list.append(_layer_name)
                    machine_list[_machine_index].download_finish_time += int(layer_stats[_layer_name]/machine_list[_machine_index].bandwidth)
                    machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
            if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:
                machine_list_copy.append(machine_list.pop(_machine_index))                    
    while(len(machine_list_copy) != machine_number):
        machine_list_copy.append(machine_list.pop(0))
#    container_counter = container_counter_copy
    # C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list_copy]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    print([machine.container_list for machine in machine_list_copy])
    print([machine.current_container_num for machine in machine_list_copy])   
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list_copy[_machine_index].layer_list])  for _machine_index in range(len(machine_list_copy))]
    return layer_size_per_machine,total_time

def schedule_container_group_greedy_machine_in_sidney_sequence(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    _group_queue = group_queue
    machine_list = [Machine() for i in range(machine_number)]
    download_size_per_machine = sum([layer_stats[_layer_name] for _layer_name in comprised_layer_list])/machine_number
    scheduled_size = 0
    scheduled_flag = {i:0 for i in container_counter}
    current_group = []
    scheduled_layer = []
    container_counter_copy = copy.deepcopy(container_counter)
    # quit()
    C_time_list = dict()
    machine_time_list = [0 for i in range(machine_number)]
    print("download_size_per_machine:",download_size_per_machine)
    # quit()
    for _machine_index in range(machine_number):
        print()
        print(scheduled_size)
        print(machine_list[_machine_index].container_limit)
        while(scheduled_size <= download_size_per_machine * (_machine_index+1)):
            if sum([1 - scheduled_flag[i] for i in scheduled_flag]) < 1:
                break
            if sum([1 - scheduled_flag[i] for i in current_group]) < 1:
                if(_group_queue.empty()):
                    break          
                current_group = _group_queue.get()
#            print("current_group",current_group)
            #calculate current remain size and get container
            #改变bandwidth对于结果没有影响
            selected_container = 0
            least_remain_time = 1000000000000000000
            selected_container_size = 0
            unselected_container = []
            for _container_name in current_group:
                if scheduled_flag[_container_name] == 0:
#                    print("_container_name",_container_name)
                    remain_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in machine_list[_machine_index].layer_list])
#                    print(233333,"_container_name",_container_name)
#                    print("_machine_index",_machine_index,"_container_name",_container_name,remain_size)
                    remain_time = remain_size/machine_list[_machine_index].bandwidth
                    if machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage > remain_size:
                        if remain_time/min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[_container_name]) < least_remain_time:
                            least_remain_time = remain_time/min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[_container_name])
                            selected_container = _container_name
                            selected_container_size = container_size[_container_name]
                        elif remain_time/min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[_container_name]) == least_remain_time and container_size[_container_name]*min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[_container_name]) > selected_container_size*container_counter_copy[selected_container]:
                            least_remain_time = remain_time/min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[_container_name])
                            selected_container = _container_name
                            selected_container_size = container_size[_container_name]
                    else:
                        unselected_container.append(_container_name)

#            print("selected_container",selected_container,"machine_index",_machine_index)       
            if(selected_container == 0):
                if(_group_queue.empty()):
                    break    
                current_group = unselected_container.copy()
#                print("uselected_container",current_group)
                break
                 
            if(min(machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num,container_counter_copy[selected_container]) == container_counter_copy[selected_container]):
                scheduled_flag[selected_container] = 1
                current_group.remove(selected_container)
            
            
            selected_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[selected_container] if _layer_name not in machine_list[_machine_index].layer_list])
            machine_list[_machine_index].current_storage += selected_container_size            
#            print("selected_container_size:",selected_container_size)
            for _layer_name in container_stats[selected_container]:
                if _layer_name not in scheduled_layer:
                    scheduled_layer.append(_layer_name)
                    scheduled_size += layer_stats[_layer_name]
                if _layer_name not in machine_list[_machine_index].layer_list:
                    machine_list[_machine_index].layer_list.append(_layer_name)
                    machine_list[_machine_index].download_finish_time += int(layer_stats[_layer_name]/machine_list[_machine_index].bandwidth)
                    machine_list[_machine_index].layer_download_finish_time[_layer_name] = machine_list[_machine_index].download_finish_time
                    
            machine_list[_machine_index].container_list.append(selected_container)

            
            # 全放进machine中运行
            if machine_list[_machine_index].container_limit-machine_list[_machine_index].current_container_num>=container_counter_copy[selected_container]:
                machine_list[_machine_index].current_container_num = machine_list[_machine_index].current_container_num + container_counter_copy[selected_container]
                C_time_list[selected_container] = container_counter_copy[selected_container]*max([machine_list[_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[selected_container]])
                machine_list[_machine_index].container_name_num[selected_container] = container_counter_copy[selected_container]
            # 部分放进该machine中运行
            else:    
                machine_list[_machine_index].container_name_num[selected_container] = machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num
                container_counter_copy[selected_container] = container_counter_copy[selected_container] - (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)
                C_time_list[selected_container] = (machine_list[_machine_index].container_limit - machine_list[_machine_index].current_container_num)* max([machine_list[_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[selected_container]])
                machine_list[_machine_index].current_container_num = machine_list[_machine_index].container_limit
                 

            if machine_list[_machine_index].container_limit==machine_list[_machine_index].current_container_num:              
                break           
            machine_time_list[_machine_index] += C_time_list[selected_container]
    print(scheduled_size)
    for _machine_index in range(len(machine_list)):
        print("current_container_number: ",machine_list[_machine_index].current_container_num)

#    container_counter = copy.deepcopy(container_counter_copy)
#    C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
  
    
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    print([machine.storage_limit for machine in machine_list])
#    print([machine.current_container_num for machine in machine_list])
#    print([machine.container_list for machine in machine_list]) 
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list[_machine_index].layer_list])  for _machine_index in range(machine_number)]

    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
#    print(complete_time_tuple)
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
#    print(complete_time_list)
    return layer_size_per_machine,total_time,complete_time_list

# layer_stats[_layer_name][3]
# container_stats[selected_container][5]
def exact_result(group_list,container_counter,container_stats,layer_stats):
    comprised_layer = set()
    comprised_layer_size = 0
    res = []
    for group in group_list:
        container_layer = {}
        container_layer_01 = {}
        layer_size = []
        layer_name = []
        for _container_name in group:
            container_layer[_container_name] = []
            container_layer_01[_container_name] = []
            for _layer_name in container_stats[_container_name]:
                if _layer_name not in comprised_layer:
                    container_layer[_container_name].append(_layer_name)
                    if _layer_name not in layer_name:
                        layer_name.append(_layer_name)
                        layer_size.append(layer_stats[_layer_name])
        
        for layer_index,_layer_name in enumerate(layer_name):
            for _container_name in group:
                if  _layer_name in container_stats[_container_name]:
                    container_layer_01[_container_name].append(1)
                else:
                    container_layer_01[_container_name].append(0)
                    
        container_weight = []
        container_layer_01_list = []
        for _container_name in container_layer_01:
            container_layer_01_list.append(container_layer_01[_container_name])
            container_weight.append(container_counter[_container_name])
        accumulated_completion_time,_ = utils.completion_time_docplex(layer_size,container_layer_01_list,container_weight)
        res.append(accumulated_completion_time + comprised_layer_size*sum([container_counter[_container_name] for _container_name in group]))
        
        for _container_name in group:
            for _layer_name in container_stats[_container_name]:
                comprised_layer.add(_layer_name)
        comprised_layer_size = sum([layer_stats[_layer_name] for _layer_name in comprised_layer])
        
    print(sum(res))
    return res

def convert(layer_stats,image_stats,container_list_unique,mode = 0):
    if mode == 1:
        converted_image_stats = {}
        layer_image = {}
        converted_layer_stats = {}
        for _container_name in container_list_unique:
            converted_image_stats[_container_name] = list()
    
        for _container_name in container_list_unique:
            for _layer_name in image_stats[_container_name][5]:
                if _layer_name not in layer_image:
                    layer_image[_layer_name] = []
                layer_image[_layer_name].append(_container_name)
        
        for _layer_name in layer_image:
            layer_image[_layer_name] = tuple(layer_image[_layer_name])
        
        for _container_name in container_list_unique:
            for _layer_name in image_stats[_container_name][5]:
                if layer_image[_layer_name] not in converted_image_stats[_container_name]:
                    converted_image_stats[_container_name].append(layer_image[_layer_name])
        
        for _layer_name in layer_image:
            if layer_image[_layer_name] not in converted_layer_stats:
                converted_layer_stats[layer_image[_layer_name]] = 0
            converted_layer_stats[layer_image[_layer_name]] += layer_stats[_layer_name][3]
    else:
        converted_image_stats = {}
        converted_layer_stats = {}
        for _layer_name in layer_stats:
            converted_layer_stats[_layer_name] = layer_stats[_layer_name][3]
        for _container_name in container_list_unique:
            converted_image_stats[_container_name] = list()
            for _layer_name in image_stats[_container_name][5]:
                converted_image_stats[_container_name].append(_layer_name)
    return converted_layer_stats,converted_image_stats

def k8s_container_limit(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)]
    container_list = []
    container_counter_copy = copy.deepcopy(container_counter)
    for container in container_counter_copy.keys():
        while container_counter_copy[container] > 0:
            container_list.append(container)
            container_counter_copy[container] -=1
#    print(container_list)
    random.shuffle(container_list)


    for _container in container_list:
        scheduled_flag = 0
        machine_put_list = []
        for _machine_index,_ in enumerate(machine_list): 
            add_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container] if _layer_name not in machine_list[_machine_index].layer_list])           
            if(add_container_size<= machine_list[_machine_index].storage_limit - machine_list[_machine_index].current_storage and machine_list[_machine_index].current_container_num<machine_list[_machine_index].container_limit):
                machine_put_list.append(_machine_index)
        # print("machine_put_list",machine_put_list)   
        if(len(machine_put_list) == 0):
            continue

        for _machine_index in machine_put_list:
            if(_container in machine_list[_machine_index].container_list):
                machine_list[_machine_index].current_container_num +=1
                machine_list[_machine_index].container_name_num[_container] += 1                
                scheduled_flag = 1
                break

        if(scheduled_flag == 0):
            min_container_limit = 500*1024*1024*1024
            min_index = 0
            for _machine_index in machine_put_list:
                if(machine_list[_machine_index].current_container_num <min_container_limit):
                    min_container_limit = machine_list[_machine_index].current_container_num
                    min_index = _machine_index
            # quit()
            machine_list[min_index].container_list.append(_container)  
            machine_list[min_index].current_storage += sum([layer_stats[_layer_name] for _layer_name in container_stats[_container] if _layer_name not in machine_list[min_index].layer_list])
            machine_list[min_index].current_container_num += 1    
            machine_list[min_index].container_name_num[_container] = 1
            for _layer_name in container_stats[_container]:
                if _layer_name not in machine_list[min_index].layer_list:
                    machine_list[min_index].layer_list.append(_layer_name)
                    machine_list[min_index].download_finish_time += int(layer_stats[_layer_name]/machine_list[min_index].bandwidth)
                    machine_list[min_index].layer_download_finish_time[_layer_name] = machine_list[min_index].download_finish_time

    # print([_machine.container_name_num for _machine in machine_list])
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list]    
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    # print(C_time_list)
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list[_machine_index].layer_list])  for _machine_index in range(machine_number)]
    
    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
    # print(complete_time_tuple)
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
    
    return layer_size_per_machine,total_time,complete_time_list
def round_robin(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,container_stats,layer_stats):
    random.seed(seed)
    np.random.seed(seed)
    machine_list = [Machine() for i in range(machine_number)] 
    visit_sequence = np.random.permutation(len(container_list_unique))
    container_counter_copy = copy.deepcopy(container_counter)  
    C_time_list = dict()
    _current_machine_index = 0
    for i in visit_sequence:        
        _container_name = container_list_unique[i]
        _selected_machine_index = -1
        for _machine_index in range(_current_machine_index,machine_number+_current_machine_index):
            _machine_index = _machine_index % machine_number
            _machine = machine_list[_machine_index]
            add_container_size = sum([layer_stats[_layer_name] for _layer_name in container_stats[_container_name] if _layer_name not in _machine.layer_list])            
            if(add_container_size<= _machine.storage_limit - _machine.current_storage and machine_list[_machine_index].container_limit-machine_list[_machine_index].current_container_num > 0):
                _selected_machine_index = _machine_index
                _current_machine_index = _selected_machine_index + 1
                break
        if(_selected_machine_index != -1):
            for _layer_name in  container_stats[_container_name]:
                if _layer_name not in machine_list[_selected_machine_index].layer_list:
                    machine_list[_selected_machine_index].layer_list.append(_layer_name)
                    machine_list[_selected_machine_index].download_finish_time += layer_stats[_layer_name]/machine_list[_selected_machine_index].bandwidth
                    machine_list[_selected_machine_index].layer_download_finish_time[_layer_name] = machine_list[_selected_machine_index].download_finish_time
            machine_list[_selected_machine_index].container_list.append(_container_name)
            machine_list[_selected_machine_index].current_storage += add_container_size
            if machine_list[_selected_machine_index].container_limit-machine_list[_selected_machine_index].current_container_num>=container_counter_copy[_container_name]:
                machine_list[_selected_machine_index].current_container_num = machine_list[_selected_machine_index].current_container_num + container_counter_copy[_container_name]
                C_time_list[_container_name] = container_counter_copy[_container_name]*max([machine_list[_selected_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                machine_list[_selected_machine_index].container_name_num[_container_name] = container_counter_copy[_container_name]
            else:    
                machine_list[_selected_machine_index].container_name_num[_container_name] = machine_list[_selected_machine_index].container_limit - machine_list[_selected_machine_index].current_container_num
                container_counter_copy[_container_name] = container_counter_copy[_container_name] - (machine_list[_selected_machine_index].container_limit - machine_list[_selected_machine_index].current_container_num)
                C_time_list[_container_name] = (machine_list[_selected_machine_index].container_limit - machine_list[_selected_machine_index].current_container_num)* max([machine_list[_selected_machine_index].layer_download_finish_time[_layer_name] for _layer_name in container_stats[_container_name]])
                machine_list[_selected_machine_index].current_container_num = machine_list[_selected_machine_index].container_limit
    print(sum(list(C_time_list.values())))
    
    # C_time_list = [utils.get_single_machine_total_weighted_time_by_assign_sequence(_machine.bandwidth,_machine.container_name_num,_machine.layer_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
     
    C_time_list = [utils.get_single_machine_total_weighted_time(_machine.bandwidth,_machine.container_name_num,_machine.container_list,container_stats,layer_stats,container_counter) for _machine in machine_list]
    total_time = sum(sum([j[0]*j[1] for j in i.values()]) for i in C_time_list)
    
    print([machine.container_list for machine in machine_list])
    print([machine.current_container_num for machine in machine_list])     
    layer_size_per_machine = [sum([layer_stats[_layer_name]/1024/1024/1024 for _layer_name in machine_list[_machine_index].layer_list])  for _machine_index in range(len(machine_list))]
    
    complete_time_tuple = []
    for d in C_time_list:
        complete_time_tuple = complete_time_tuple + list(d.values())
    print(complete_time_tuple)
    complete_time_list = []
    for tu in complete_time_tuple:
        index = tu[0]
        while(index>0):
            complete_time_list.append(tu[1])
            index -= 1
    print(complete_time_list)
    return layer_size_per_machine,total_time,complete_time_list

_start_time = time.time()
distribution = "zipf"
machine_number = 15
container_number = 200
exp_num_C = 10
# 1 为 layer grouping
convert_mode = 1
#hyperparameter for tradeoff  [559383740869.0, 946909327857.0, 631102367242.5, 352094465467.0, 361950762552.5, 354749197880.5]
alpha = 0.5

scheduling_result = []

tr = Trace()
cdf = [[],[],[],[]]
random.seed(2)
ab1 = 0
abc1 = 0
abcd1 = 0
ab2 = 0
abc2 = 0
abcd2 = 0
ab3 = 0
abc3 = 0 
abcd3 = 0
ab4 = 0
abc4 = 0 
abcd4 = 0 
ab6 = 0
abc6 = 0 
abcd6 = 0  
ab7 = 0
abc7 = 0 
abcd7 = 0 

for exp_num in range(exp_num_C):
    random.seed(exp_num)
    

    # 生成数据
    if distribution == "uniform":
        container_list = [
            random.sample(tr.image_name_list, 1)[0]
            for i in range(container_number)
        ]
    elif distribution == "zipf":
        np.random.seed(1)
#        random.seed(4)
        zipf_param = 1.1
        container_list = []
        image_name_list = []
        image_name_list.extend(tr.image_name_list)
        random.shuffle(image_name_list)
        sample_result = []
        while(len(sample_result) < container_number):
            s = np.random.zipf(zipf_param, container_number-len(sample_result))
            for i in s:
                if i <= len(image_name_list):
                    sample_result.append(int(i-1))
        for sample_index in sample_result:
            container_list.append(image_name_list[sample_index])

    # 将同样type的container统计到一起        
    container_number = len(container_list)
    container_list_unique = list()
    for i in container_list:
        if i not in container_list_unique:
            container_list_unique.append(i)
    container_counter = collections.Counter()
    container_counter.update(container_list)
    container_size = {i:0 for i in container_counter}
    
    # layer grouping
    converted_layer_stats,converted_image_stats = convert(tr.layer_stats,tr.image_stats,container_list_unique,mode = convert_mode)
    # print(container_number)
    print (len(converted_image_stats))
    # quit()
    comprised_layer_list = list()
    for _container_name in container_counter:
        for _layer_name in converted_image_stats[_container_name]:
            if _layer_name not in comprised_layer_list:
                comprised_layer_list.append(_layer_name)
            container_size[_container_name] +=  converted_layer_stats[_layer_name]
    group_queue,group_list = utils.sidney_decomposition(converted_layer_stats,converted_image_stats,comprised_layer_list,container_counter)

    seed = exp_num
    print("comprised_layer_list",len(comprised_layer_list))
#    print("a1")
    a1 = schedule_container_group_greedy_machine_in_sidney_sequence(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab1 =a1[1]
    abc1 = sum(a1[0])  
    abcd1 = np.std(a1[0],ddof=1)
#    cdf[0] += a1[2]
    
    #print()
    #t1 = time.time()
    #exact_result(group_list,container_counter,converted_image_stats,converted_layer_stats)
    #print(time.time()-t1)
    #
#    print()
    a2 = schedule_container_random(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab2 =a2[1]
    abc2 = sum(a2[0])   
    abcd2 = np.std(a2[0],ddof=1)
#    cdf[1] += a2[2]    
    print()  
    a3 = round_robin(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab3 =a3[1]
    abc3 = sum(a3[0]) 
    abcd3 = np.std(a3[0],ddof=1)
#    cdf[2] += a3[2]    
    ##
    print()
    a4 = schedule_container_machine_greedy1(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab4 =a4[1]
    abc4 = sum(a4[0])   
    abcd4 = np.std(a4[0],ddof=1)
#    cdf[3] += a4[2]    
    ##
#    print()
    # a5 = schedule_container_machine_diff_greedy(seed,alpha,group_list,container_size,comprised_layer_list,group_queue, machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
#    ab5 +=a5[1]
#    ##
    a6 = schedule_container_group_machine_greedy(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab6 =a6[1]
    abc6 = sum(a6[0])   
    abcd6 = np.std(a6[0],ddof=1)
##    #schedule_container_group_machine_greedy
##    print()
    a7 = schedule_container_in_sequence(seed,alpha,group_list,container_size,comprised_layer_list,group_queue,machine_number,container_counter,container_list_unique,converted_image_stats,converted_layer_stats)
    ab7 =a7[1]
    abc7 = sum(a7[0])  
    abcd7 = np.std(a7[0],ddof=1)
    print("exp_num:",exp_num)
    res = [ab1,ab2,ab3,ab4,ab6,ab7]
    res1 = [abc1,abc2,abc3,abc4,abc6,abc7]
    res2 = [abcd1,abcd2,abcd3,abcd4,abcd6,abcd7]  
#
    file= open(r'mBandwith_randomseed_fixed_seed15.txt', mode='a+', encoding='UTF-8')
    # open()打开一个文件，返回一个文件对象
    file.write('Bandwith_randomseed_fixed_seed15.txt: ')  # 写入文件
    file.write(str(exp_num)+' ')
    file.write('container_num: ')
    file.write(str(res1))
    file.write('\n')
    file.seek(0)  # 光标移动到文件开头
    file_content = file.read()  # 读取整个文件内容
    print(file_content)
    file.close() # 关闭文件

    file= open(r'Load_Balance_randomseed_fixed_seed15.txt', mode='a+', encoding='UTF-8')
    # open()打开一个文件，返回一个文件对象
    file.write('Load_Balance_randomseed_fixed_seed15.txt: ')  # 写入文件
    file.write(str(exp_num)+' ')
    file.write('container_num: ')
    file.write(str(res2))
    file.write('\n')
    file.seek(0)  # 光标移动到文件开头
    file_content = file.read()  # 读取整个文件内容
    print(file_content)
    file.close() # 关闭文件

    file= open(r'Latency_randomseed_fixed_seed15.txt', mode='a+', encoding='UTF-8')
    # open()打开一个文件，返回一个文件对象
    file.write('Latency_randomseed_fixed_seed15.txt: ')  # 写入文件
    file.write(str(exp_num)+' ')
    file.write('container_num: ')
    file.write(str(res))
    file.write('\n')
    file.seek(0)  # 光标移动到文件开头
    file_content = file.read()  # 读取整个文件内容
    print(file_content)
    file.close() # 关闭文件
    # print(res)
    # res = [ab1/exp_num_C,ab2/exp_num_C,ab3/exp_num_C,ab4/exp_num_C,ab6/exp_num_C,ab7/exp_num_C]
    # res1 = [abc1/exp_num_C,abc2/exp_num_C,abc3/exp_num_C,abc4/exp_num_C,abc6/exp_num_C,abc7/exp_num_C]
    # res2 = [abcd1/exp_num_C,abcd2/exp_num_C,abcd3/exp_num_C,abcd4/exp_num_C,abcd6/exp_num_C,abcd7/exp_num_C]
    # print()
    # print(res)
    # print()
    # print(res1)
    # print()
    # print(res2)


# plt.rc('font',family='Times New Roman')
# input_values=[10,15,20,25,30]
# A=np.array([[130233.46932136535, 96820.88579666137, 80553.75159973143, 76497.26564521789, 82253.05126266477, 79329.82163864134]
# ,[105554.97332824708, 82894.2235748291, 68905.98637390135, 64387.20182701109, 67726.78162727357, 66476.54112190247]
# ,[89142.45096031188, 77235.11260223387, 62421.15746459961, 59266.813591156, 61333.212909088135, 60592.79862602234]
# ,[80878.50405967711, 70565.97450241089, 59059.440935287486, 56628.02484703064, 57970.87233001708, 57152.382841339124]
# ,[74082.60149528504, 67963.1049055481, 56503.84672721863, 54959.41224380493, 55968.52082489014, 55192.10984252929]])

# plt.plot(input_values,A[:,3],"-D",linewidth=2,label="LASA")
# plt.plot(input_values,A[:,0],"-s",linewidth=2,label="A1 & GLSA")
# plt.plot(input_values,A[:,1],"-v",linewidth=2,label="A2 & GLSA")
# plt.plot(input_values,A[:,2],"-o",linewidth=2,label="A3 & GLSA")
# plt.plot(input_values,A[:,4],"-*",linewidth=2,label="A4 & GLSA")
# ax=plt.gca()
# ax.ticklabel_format(style='sci', scilimits=(-1,2), axis='y')
# #plt.plot(input_values,squares1,linewidth=2)
# # 设置图表标题，并给坐标轴加上标签
# plt.xlabel("Number of Edge Nodes",fontsize=14)
# plt.ylabel("Total Startup Time (s)",fontsize=14)
# # 设置刻度标记的大小
# plt.tick_params(axis='both',labelsize=14)
# plt.legend()
# plt.savefig('assignment_compare_Startup_time'+distribution+'.pdf',bbox_inches='tight')
# plt.show()


# plt.rc('font',family='Times New Roman')
# input_values=[10,15,20,25,30]
# A=np.array([[17.669269020017236, 20.324680312070996, 16.962065372243522, 16.394154345802963, 18.1965641788207, 17.658347545657307]
# ,[18.639874935895204, 21.12487795324996, 17.704376451577993, 17.246455804165453, 18.74605205617845, 18.432975020911545]
# ,[19.129311878141017, 21.599256704654543, 18.545514277461915, 17.882796353660524, 19.213499489147218, 19.175438186340035]
# ,[19.922463187202812, 21.323315068054946, 19.02127041919157, 18.46190254194662, 19.902331301756202, 19.625917000696063]
# ,[20.32232057163492, 21.961580210272224, 19.685247415676713, 18.74350450374186, 20.707088581752032, 20.329547622799872]])

# plt.plot(input_values,A[:,3],"-D",linewidth=2,label="LASA")
# plt.plot(input_values,A[:,0],"-s",linewidth=2,label="A1 & GLSA")
# plt.plot(input_values,A[:,1],"-v",linewidth=2,label="A2 & GLSA")
# plt.plot(input_values,A[:,2],"-o",linewidth=2,label="A3 & GLSA")
# plt.plot(input_values,A[:,4],"-*",linewidth=2,label="A4 & GLSA")
# #plt.plot(input_values,squares1,linewidth=2)
# # 设置图表标题，并给坐标轴加上标签
# plt.xlabel("Number of Edge Nodes",fontsize=14)
# plt.ylabel("Bandwidth Usage (GB)",fontsize=14)
# # 设置刻度标记的大小
# plt.tick_params(axis='both',labelsize=14)
# plt.legend()
# plt.savefig('assignment_compare_Resource_Utilization'+distribution+'.pdf',bbox_inches='tight')
# plt.show()


# plt.rc('font',family='Times New Roman')
# input_values=[10,15,20,25,30]
# A=np.array([[1.0046563586319028, 0.9971687503425077, 0.3572708232723831, 0.6061956115132665, 0.6699285571821567, 0.6499475877554329]
# ,[0.6569573115951377, 0.9146274501814581, 0.305501650272139, 0.42294962469530617, 0.4335815933137706, 0.43603462819080746]
# ,[0.5198208934408945, 0.7987301098664946, 0.2789547050680542, 0.36455839841590476, 0.36151375863094215, 0.362450797650383]
# ,[0.4693338983056269, 0.6915931524479093, 0.2663144929521288, 0.3381681337522792, 0.33168060213337747, 0.330507324091621]
# ,[0.42296899148125816, 0.607454132987544, 0.26598240322922495, 0.32956840958235367, 0.31146185593909415, 0.3104624188374816]])
# plt.plot(input_values,A[:,3],"-D",linewidth=2,label="LASA")
# plt.plot(input_values,A[:,0],"-s",linewidth=2,label="A1 & GLSA")
# plt.plot(input_values,A[:,1],"-v",linewidth=2,label="A2 & GLSA")
# plt.plot(input_values,A[:,2],"-o",linewidth=2,label="A3 & GLSA")
# plt.plot(input_values,A[:,4],"-*",linewidth=2,label="A4 & GLSA")
# #plt.plot(input_values,squares1,linewidth=2)
# # 设置图表标题，并给坐标轴加上标签
# plt.xlabel("Number of Edge Nodes",fontsize=14)
# plt.ylabel("Load Balance",fontsize=14)
# # 设置刻度标记的大小
# plt.tick_params(axis='both',labelsize=14)
# plt.legend()
# plt.savefig('assignment_compare_Load_Balance'+distribution+'.pdf',bbox_inches='tight')
# plt.show()




