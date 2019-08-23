# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# sql = """create table weichai_tab(id int, engine_id char(20), brand char(20),power char(20),frequency char(20),rated_power char(20),fuel char(20),series char(20),adaptation_scope char(20), Minimum_fuel_consumption char(20), displacement char(20), output_standard char(20),technical_route char(100),Admission_mehtod char(20),maximum_power_output char(20),rated_speed char(20),Max_Hp char(20),maximum_torque char(20), maximum_torque_speed char(20),engine_type char(20),Valve_train char(20),cylinder_num char(20),Bore_stroke char(20),Engine_Dimension char(20),weight char(20),torque char(20)) character set utf8
#                         """
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
'''参数名 {'转速（rpm)', '机油消耗率', '发火顺序', '最大马力：', '最大扭矩转速：', '汽缸排列形式：',
 '排量', '技术路线：式(电控泵)', '未知类型0', '机组功率（KW）', '燃料及配置0#柴油', '机组型号', '最大输出功率：',
  '适配范围：', '型式', 功率（kW）', '频率（HZ）', '标定功率（kW/Ps）', '燃料', '发动机品牌：', '进气方式', '每缸气门数：',
   '单缸气门数', '最'排量：', '额定功率/转速（kW/rpm）', '机组持续功率/kW', '汽缸数：', '超负荷功率/转速（N.m/rpm）',
   '功率(kW）', '电机械泵）', '发动机尺寸：', '净重（t)', '机组常用功率/kW', '标定功率(kW/Ps）',
   '卡车杂谈>玉柴发动机气缸垫子漏水是通病？技术路线',
   '缸径/行程', '外特性最低燃油消耗率（g/kW·h）',
    '柴油机型号', '发动机参数',
    '曲轴旋转方向（面向自由端）', 速（kw/rpm）',
     '机型', '型号', '喷油系统：', '外形尺寸/mm',
     '转速（r/min）',
     '压缩比：', '发动机净重：', '噪声', '发：', '烟度', '缸径x行程：',
     '排放标准', '排放阶段', '峰值功率：',
      '扭矩：', '排量（L）', '净重/kg', '排放标准：',
       '供', '全负荷最低燃油耗率：', '燃料种类：', '发动机厂商：', '排量(L)', '发动机形式：'}
'''
import re
import json
import pymysql
import time
class WeichaiPipeline(object):
    def __init__(self):
        self.count = 0
        self.filename = open("xx.json", "wb")
        self.con = pymysql.connect(host='localhost',
                                   user='root',
                                   passwd='123',
                                   charset='utf8mb4'
                                   )
        self.cur = self.con.cursor()
        self.cur.execute('drop database if exists weichai_db')
        self.cur.execute('create database weichai_db character set utf8mb4')
        self.cur.execute('use weichai_db')
        self.cur.execute('drop table if exists weichai_tab')
        sql = 'create table weichai_tab(id char(20), engine_id char(20), brand char(20),factory char(20),engine_type char(100),frequency char(20),rated_power char(20),fuel char(20),series char(20),compression_ratio char(20),adaptation_scope char(20), Minimum_fuel_consumption char(20), displacement char(20), commen_power char(20),output_standard char(20),noise char(20),continuous_power char(20),technical_route char(100),Admission_mehtod char(20),maximum_power_output char(20),rated_speed char(20),Max_Hp char(20),maximum_torque char(20), maximum_torque_speed char(20),Valve_train char(20),cylinder_num char(20),cylinder_type char(20),Bore_stroke char(20),Engine_Dimension char(20),weight char(20),torque char(20),rated_power_speed_rate char(20),max_power_speed_rate char(20)) character set utf8mb4'
        self.cur.execute(sql)

    def process_item(self, item, spider):
        a = item['detail']
        list = []
        for i in a:
            for k in i.keys():
                list.append(k)
        s = set(list)
        if len(s) == len(list):#长度相等说明detail里的数据是一台发动机的，所以要把所有的东西合并到一个字典里
            b = a[0]
            for i in a:
                if i != a[0]:
                    b.update(i) #完成合并 b 是新的长字典
            jsontext = json.dumps(b, ensure_ascii=False) + ",\n"
            self.filename.write(jsontext.encode("utf-8"))
            value = [b[k] for k in b.keys()] #获取value
            keys = [self.name_map(key.strip('：')) for key in b.keys()] #将参数名映射到标准参数名
            new_keys = []
            new_value = []
            for id, m in enumerate(keys): #剔除无效参数名
                if m != 0:
                    new_keys.append(m)
                    new_value.append(value[id])
            self.count += 1
            new_keys.insert(0,'id')
            new_value.insert(0,str(self.count))
            aa = ','.join(new_keys)
            b = ','.join(['"'+v+'"' for v in new_value])
            try:
                sql_insert_1 = 'insert into weichai_tab(%s)' % (aa)
                sql_insert_2 = ' values(%s)'%(b)
                print(sql_insert_1 + sql_insert_2)
                self.cur.execute(sql_insert_1 + sql_insert_2)
                self.con.commit()
                self.cur.execute('select * from weichai_tab')
                result = self.cur.fetchall()
                for row in result:
                    print('数据库写入一条数据：' ,row)
            except Exception as e:
                print('错误：', e)
                self.con.rollback()
        else: #说明有多台发动机
            for i in a: #每条都是一个发动机数据，直接存 i是字典
                print(i)
                jsontext = json.dumps(i, ensure_ascii=False) + ",\n"
                self.filename.write(jsontext.encode("utf-8"))
                value = [i[k] for k in i.keys()]
                keys = [self.name_map(key.strip('：')) for key in i.keys()]
                new_keys = []
                new_value = []
                for id, m in enumerate(keys):
                    if m != 0:
                        new_keys.append(m)
                        new_value.append(value[id])
                self.count += 1
                new_keys.insert(0, 'id')
                new_value.insert(0, str(self.count))
                aa = ','.join(new_keys)
                b = ','.join(['"' + v + '"' for v in new_value])
                try:
                    sql_insert_1 = 'insert into weichai_tab(%s)' % (aa)
                    sql_insert_2 = ' values(%s)'%(b)
                    print(sql_insert_1 + sql_insert_2)
                    self.cur.execute(sql_insert_1 + sql_insert_2)
                    self.con.commit()
                    self.cur.execute('select * from weichai_tab')
                    result = self.cur.fetchall()
                    for row in result:
                        print('数据库写入一条数据：',row )
                except Exception as e:
                    print('错误：', e)
                    self.con.rollback()
        return item
    def name_map(self,name):
        '''柴油机型号，生产厂家，用途，结构形式，气缸数目，缸径，行程，行程/缸径，排量，
        标定功率，备用功率，系列最大标定功率，标定转速，最大扭矩，最大扭矩初始转速，
        最大扭矩终止转速，大功率机型最大扭矩，低速扭矩，压缩比，排放控制技术路线，排放水平，
        宣传重量，宣传长度，宣传宽度，宣传高度，B10寿命，大修期，DPF清灰周期，气门调整周期，
        机油更换周期，机油容量_高，机油容量_低，最大制动功率，高原能力，冷启动，扭矩储备率，超级扭矩'''
        list = ['机型 机组型号 型号 柴油机型号 发动机', '品牌 发动机品牌', '厂商 发动机厂商','系列', '发动机形式','功率 机组功率 额定功率 标定功率', '最大输出功率 最大功率 峰值功率 超负荷功率', '频率', '燃料', '烟度', '最低燃',
                '压缩比', '尺寸 长x宽x高 长*宽*高 发动机尺寸', '发动机净重 净重 重量', '排放', '排量', '噪声', '额定转速 标定转速', '缸径', '马力', '持续功率', '单缸气门数 每缸气门数',
                '气缸数', '机组常用功率', '气缸排列 气缸形式', '技术路线', '适用范围', '进气方式', '最大扭矩', '额定功率/转速', '超负荷功率/转速', '最大扭矩转速', '扭矩']
        list2 = ['engine_id', 'brand','factory','series','engine_type', 'rated_power', 'maximum_power_output', 'frequency', 'fuel', 'smoke',
                 'Minimum_fuel_consumption', 'compression_ratio', 'Engine_Dimension', 'weight', 'output_standard',
                 'displacement', 'nosie', 'rated_speed', 'Bore_stroke', 'Max_Hp', 'continuous_power', 'Valve_train',
                 'cylinder_num', 'commen_power', 'engine_type', 'technical_route', 'adaptation_scope',
                 'Admission_mehtod', 'maximum_torque', 'rated_power_speed_rate', 'max_power_speed_rate',
                 'maximum_torque_speed', 'torque']
        m = name
        temp = ['', -1]
        for index, i in enumerate(list):#index 是参数匹配列表的索引，i是参数匹配字符段
            a = i.split(' ') #把字符段切分成单个匹配对象
            for j in a: #对每一个匹配字段
                pattern = re.compile(u'%s' % j)
                match = pattern.findall(m)
                if match and len(temp[0]) < len(j):
                    print(m,j,list2[index])
                    temp[0] = j
                    temp[1] = index
                else:
                    pass
        if temp[1] != -1:
            return list2[temp[1]]
        else:
            return 0


    def close_spider(self, spider):
        # self.con.close()
        self.filename.close()
# sql = """create table weichai_tab(id int, engine_id char(20), brand char(20),
# power char(20),frequency char(20),rated_power char(20),fuel char(20),
# series char(20),adaptation_scope char(20), Minimum_fuel_consumption char(20),
# displacement char(20), output_standard char(20),technical_route char(100),
# Admission_mehtod char(20),maximum_power_output char(20),rated_speed char(20),
# Max_Hp char(20),maximum_torque char(20), maximum_torque_speed char(20),
# engine_type char(20),Valve_train char(20),cylinder_num char(20),Bore_stroke char(20),
# Engine_Dimension char(20),weight char(20),torque char(20)) character set utf8
#                         """
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
'''参数名 {'转速（rpm)', '机油消耗率', '发火顺序', '最大马力：', '最大扭矩转速：', '汽缸排列形式：',
 '排量', '技术路线：式(电控泵)', '未知类型0', '机组功率（KW）', '燃料及配置0#柴油', '机组型号', '最大输出功率：',
  '适配范围：', '型式', 功率（kW）', '频率（HZ）', '标定功率（kW/Ps）', '燃料', '发动机品牌：', '进气方式', '每缸气门数：',
   '单缸气门数', '最'排量：', '额定功率/转速（kW/rpm）', '机组持续功率/kW', '汽缸数：', '超负荷功率/转速（N.m/rpm）',
   '功率(kW）', '电机械泵）', '发动机尺寸：', '净重（t)', '机组常用功率/kW', '标定功率(kW/Ps）',
   '卡车杂谈>玉柴发动机气缸垫子漏水是通病？技术路线',
   '缸径/行程', '外特性最低燃油消耗率（g/kW·h）',
    '柴油机型号', '发动机参数',
    '曲轴旋转方向（面向自由端）', 速（kw/rpm）',
     '机型', '型号', '喷油系统：', '外形尺寸/mm',
     '转速（r/min）',
     '压缩比：', '发动机净重：', '噪声', '发：', '烟度', '缸径x行程：',
     '排放标准', '排放阶段', '峰值功率：',
      '扭矩：', '排量（L）', '净重/kg', '排放标准：',
       '供', '全负荷最低燃油耗率：', '燃料种类：', '发动机厂商：', '排量(L)', '发动机形式：'}
'''
'''柴油机型号，生产厂家，用途，结构形式，气缸数目，缸径，行程，行程/缸径，排量，
标定功率，备用功率，系列最大标定功率，标定转速，最大扭矩，最大扭矩初始转速，
最大扭矩终止转速，大功率机型最大扭矩，低速扭矩，压缩比，排放控制技术路线，排放水平，
宣传重量，宣传长度，宣传宽度，宣传高度，B10寿命，大修期，DPF清灰周期，气门调整周期，
机油更换周期，机油容量_高，机油容量_低，最大制动功率，高原能力，冷启动，扭矩储备率，超级扭矩'''