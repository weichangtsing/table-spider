import scrapy
import re
import numpy as np
import pymysql
import time
from weichai.items import weichaiitem
'''spider 进行网页数据的爬取，item定义需要传出的页面表格信息，在pipeline中对传出的item信息进行处理，以json格式写入本地文件'''
class Spider(scrapy.Spider):
    name = "weichai"

    def start_requests(self):
        self.table = []
        self.attr = '功率' #设定关键词，由发动机性能表格一定会包含的参数值定位到表格

        urls = []
        # with open('weichai/spiders/changjia','r',encoding='utf8') as f: #从厂家txt文件中读取出厂家网站链接
        #     for i in f.readlines():
        #         urls.append(i)
        urls = ['https://www.weichai.com/cpyfw/wmdyw/dlzc/kcyfdj/','https://product.360che.com/price/c3_s61_b0_s0.html'] #待爬的网站的列表，这个在确定了目标网站之后，可以从文件中读取，这里以潍柴动力自己的网站为测试
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse1) #yield逐个产生网站， callback是指定这个链接解析之后的东西送往哪一个函数

    def parse1(self, response):  # 进行第一级页面爬取
        item = weichaiitem()
        list = ['"功率"', '"气缸"']
        table = []
        for st in list:
            try:
                num = 0  # 行数统计
                count = 0  # 无内容单元格 未知类型统计
                  # 用于存放页面中的所有的表格
                con = '//tr[contains(string(),%s)]/..'%(st)
                a = response.xpath(con)  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
                for t in a:  # 因为页面中可能回存在多个表格。全部抽取出来
                    try:
                        m = []
                        for i in a.xpath('./*'):  # 定位到每行
                            m.append([])  # 每个列表代表每一行的元素，对列表的每一行的循环中增加一个列表用来存放每行元素
                            b = i.xpath(
                                './*')  # 定位到行中的每个列单元格j就是行的每一个列元素 但是一个列元素肯能会包含多个子元素，因为存在单位的情况，这里我先不讨论单位，直接把其合并为同一个元素
                            for j in b:
                                c = j.xpath('string(.)').extract()[0]
                                c = c.replace('\n', '').replace(' ', '').replace('\xa0', '')
                                print(c)
                                if c != '':  # 判断是不是空单元格，若不是则直接添加进列表
                                    m[num].append(c.replace('\n', '').replace(' ', '').replace('\xa0', ''))
                                else:  # 如果是空单元格，则添加未知类型
                                    m[num].append('未知类型' + str(count))
                                    count += 1
                            num += 1
                        # 剔除无用标题行
                        k = [len(i) for i in m]
                        mm = max(k)  # 找到最小长度的标题行
                        new_m = []
                        for i in m:
                            if len(i) == mm:
                                new_m.append(i)
                            else:
                                pass
                        m = new_m
                        table.append(m)
                    except:
                        pass
            except:
                pass
        if len(table) > 0:
            print('当前页面抽取到%s个表格！' % len(table))
            print('====================')
            print(response.request.url)
        pattern1 = re.compile(
            u'发动机|系列|频率|外形|净重|功率|气门|方式|缸|机型|型式|型号|排量|最大|扭矩|转速|最低|燃油|消耗|技术|怠速|型|额定|进气|适配|烟度|排放|未知|类型|噪声|厂|品牌|缸径|行程|宽|排放|种类|燃料')  # 用户判断是不是包含参数名类型
        pattern2 = re.compile('[0-9]+')  # 用于判断是不是包含数组
        for m in table:
            n = []  # 创建一个新列表用来存放单元格类型判断的结果
            num = 0  # 用于n列表的行计数
            for i in m:
                n.append([])  # m每有一行，n也要添加一行
                for j in i:
                    match1 = pattern1.findall(j)  # 用于匹配参数名
                    match2 = pattern2.findall(j)  # 用于匹配数字
                    if match1 and bool(match2) != True:  # 进行判断，如果包含参数名的字符，并且没有数字，则判定是参数名
                        n[num].append(1)
                    else:  # 否则则判定为参数值
                        n[num].append(0)
                num += 1  # 行计数加一
            detail = []  # 存放每一页的爬的表格信息
            count_r = []
            count_c = []
            for id, i in enumerate(n):  # 进行行统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_r.append(id)  # 记录标题行的行号
            n1 = np.array(n).T
            n = n1.tolist()
            for id, i in enumerate(n):  # 进行列统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_c.append(id)
            if 0 in count_r and 0 not in count_c:  # 用第一行或者列做检验，如果如果在，首先证明，不可能是空的，如果第一行或者列在计数列表中，那么肯定不是行标题或者列标题
                print('检测出行表格，正在进行结构化数据抽取...')
                try:
                    for id, i in enumerate(count_r):  # i是标题行的行号
                        if i != count_r[-1]:  # 如果i不是最后一个标题行，这就意味着，i到i+1行之间的就是参数内容
                            for mm in m[i + 1:count_r[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail', 'a+', encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)
                except:
                    pass
            elif 0 in count_c and 0 not in count_r:  # 进行列标题的抽取。
                print('检测出列表格，正在进行结构化数据抽取...')
                try:
                    m = np.array(m).T
                    for id, i in enumerate(count_c):  # i是标题列的列号
                        if i != count_c[-1]:  # 如果i不是最后一个标题列，这就意味着，i到i+1列之间的就是参数内容
                            for mm in m[i + 1:count_c[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail', 'a+', encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)

                except Exception as e:
                    print(e)
                    pass
            item['detail'] = detail
            yield item

        b = response.xpath('//*/@href')  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
        for i in b.extract():
            url = response.urljoin(i)
            print('链接',url)
            try:
                yield scrapy.Request(url=url, callback=self.parse2)  # 将当前页面抽取出的信息送去二级页面进行数据爬取
            except Exception as e:
                print('错误：', e)
                pass
        c = response.xpath('//*[contains(text(),"下一页")]/@href').extract_first()
        url = response.urljoin(c)
        try:
            yield scrapy.Request(url=url, callback=self.parse1)
            print('下一页')
        except Exception as e:
            print('错误：', e)
            pass

    def parse2(self, response):  # 进行第一级页面爬取
        item = weichaiitem()
        list = ['"功率"', '"气缸"']
        table = []
        for st in list:
            try:
                num = 0  # 行数统计
                count = 0  # 无内容单元格 未知类型统计
                # 用于存放页面中的所有的表格
                con = '//tr[contains(string(),%s)]/..' % (st)
                a = response.xpath(con)  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
                for t in a:  # 因为页面中可能回存在多个表格。全部抽取出来
                    try:
                        m = []
                        for i in a.xpath('./*'):  # 定位到每行
                            m.append([])  # 每个列表代表每一行的元素，对列表的每一行的循环中增加一个列表用来存放每行元素
                            b = i.xpath(
                                './*')  # 定位到行中的每个列单元格j就是行的每一个列元素 但是一个列元素肯能会包含多个子元素，因为存在单位的情况，这里我先不讨论单位，直接把其合并为同一个元素
                            for j in b:
                                c = j.xpath('string(.)').extract()[0]
                                c = c.replace('\n', '').replace(' ', '').replace('\xa0', '')
                                if c != '':  # 判断是不是空单元格，若不是则直接添加进列表
                                    m[num].append(c.replace('\n', '').replace(' ', '').replace('\xa0', ''))
                                else:  # 如果是空单元格，则添加未知类型
                                    m[num].append('未知类型' + str(count))
                                    count += 1
                            num += 1
                        # 剔除无用标题行
                        k = [len(i) for i in m]
                        mm = max(k)  # 找到最小长度的标题行
                        new_m = []
                        for i in m:
                            if len(i) == mm:
                                new_m.append(i)
                            else:
                                pass
                        m = new_m
                        table.append(m)
                    except:
                        pass
            except:
                pass
        if len(table) > 0:
            print('当前页面抽取到%s个表格！' % len(table))
            print('====================')
            print(response.request.url)
        pattern1 = re.compile(
            u'发动机|系列|频率|外形|净重|功率|气门|方式|缸|机型|型式|型号|排量|最大|扭矩|转速|最低|燃油|消耗|技术|怠速|型|额定|进气|适配|烟度|排放|未知|类型|噪声|厂|品牌|缸径|行程|宽|排放|种类|燃料')  # 用户判断是不是包含参数名类型
        pattern2 = re.compile('[0-9]+')  # 用于判断是不是包含数组
        for m in table:
            n = []  # 创建一个新列表用来存放单元格类型判断的结果
            num = 0  # 用于n列表的行计数
            for i in m:
                n.append([])  # m每有一行，n也要添加一行
                for j in i:
                    match1 = pattern1.findall(j)  # 用于匹配参数名
                    match2 = pattern2.findall(j)  # 用于匹配数字
                    if match1 and bool(match2) != True:  # 进行判断，如果包含参数名的字符，并且没有数字，则判定是参数名
                        n[num].append(1)
                    else:  # 否则则判定为参数值
                        n[num].append(0)
                num += 1  # 行计数加一
            detail = []  # 存放每一页的爬的表格信息
            count_r = []
            count_c = []
            for id, i in enumerate(n):  # 进行行统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_r.append(id)  # 记录标题行的行号
            n1 = np.array(n).T
            n = n1.tolist()
            for id, i in enumerate(n):  # 进行列统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_c.append(id)
            if 0 in count_r and 0 not in count_c:  # 用第一行或者列做检验，如果如果在，首先证明，不可能是空的，如果第一行或者列在计数列表中，那么肯定不是行标题或者列标题
                print('检测出行表格，正在进行结构化数据抽取...')
                try:
                    for id, i in enumerate(count_r):  # i是标题行的行号
                        if i != count_r[-1]:  # 如果i不是最后一个标题行，这就意味着，i到i+1行之间的就是参数内容
                            for mm in m[i + 1:count_r[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail', 'a+', encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)

                except:
                    pass
            elif 0 in count_c and 0 not in count_r:  # 进行列标题的抽取。
                print('检测出列表格，正在进行结构化数据抽取...')
                try:
                    m = np.array(m).T
                    for id, i in enumerate(count_c):  # i是标题列的列号
                        if i != count_c[-1]:  # 如果i不是最后一个标题列，这就意味着，i到i+1列之间的就是参数内容
                            for mm in m[i + 1:count_c[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail', 'a+', encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)

                except Exception as e:
                    print(e)
                    pass
            self.table.append(detail)
            item['detail'] = detail
            yield item
        b = response.xpath('//*/@href')  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
        for i in b.extract():
            url = response.urljoin(i)
            print('链接', url)
            try:
                yield scrapy.Request(url=url, callback=self.parse3)  # 将当前页面抽取出的信息送去二级页面进行数据爬取
            except Exception as e:
                print('错误：', e)
                pass
        c = response.xpath('//*[contains(text(),"下一页")]/@href').extract_first()
        url = response.urljoin(c)
        try:
            yield scrapy.Request(url=url, callback=self.parse3)
            print('下一页')
        except Exception as e:
            print('错误：', e)
            pass

    def parse3(self, response):  # 进行第一级页面爬取
        item = weichaiitem()
        list = ['"功率"', '"气缸"']
        table = []
        for st in list:
            try:
                num = 0  # 行数统计
                count = 0  # 无内容单元格 未知类型统计
                  # 用于存放页面中的所有的表格
                con = '//tr[contains(string(),%s)]/..'%(st)
                a = response.xpath(con)  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
                for t in a:  # 因为页面中可能回存在多个表格。全部抽取出来
                    try:
                        m = []
                        for i in a.xpath('./*'):  # 定位到每行
                            m.append([])  # 每个列表代表每一行的元素，对列表的每一行的循环中增加一个列表用来存放每行元素
                            b = i.xpath(
                                './*')  # 定位到行中的每个列单元格j就是行的每一个列元素 但是一个列元素肯能会包含多个子元素，因为存在单位的情况，这里我先不讨论单位，直接把其合并为同一个元素
                            for j in b:
                                c = j.xpath('string(.)').extract()[0]
                                c = c.replace('\n', '').replace(' ', '').replace('\xa0', '')
                                if c != '':  # 判断是不是空单元格，若不是则直接添加进列表
                                    m[num].append(c.replace('\n', '').replace(' ', '').replace('\xa0', ''))
                                else:  # 如果是空单元格，则添加未知类型
                                    m[num].append('未知类型' + str(count))
                                    count += 1
                            num += 1
                        # 剔除无用标题行
                        k = [len(i) for i in m]
                        mm = max(k)  # 找到最小长度的标题行
                        new_m = []
                        for i in m:
                            if len(i) == mm:
                                new_m.append(i)
                            else:
                                pass
                        m = new_m
                        table.append(m)
                    except:
                        pass
            except:
                pass
        if len(table) > 0:
            print('当前页面抽取到%s个表格！' % len(table))
            print('====================')
            print(response.request.url)
        pattern1 = re.compile(
            u'发动机|系列|频率|外形|净重|功率|气门|方式|缸|机型|型式|型号|排量|最大|扭矩|转速|最低|燃油|消耗|技术|怠速|型|额定|进气|适配|烟度|排放|未知|类型|噪声|厂|品牌|缸径|行程|宽|排放|种类|燃料')  # 用户判断是不是包含参数名类型
        pattern2 = re.compile('[0-9]+')  # 用于判断是不是包含数组
        for m in table:
            n = []  # 创建一个新列表用来存放单元格类型判断的结果
            num = 0  # 用于n列表的行计数
            for i in m:
                n.append([])  # m每有一行，n也要添加一行
                for j in i:
                    match1 = pattern1.findall(j)  # 用于匹配参数名
                    match2 = pattern2.findall(j)  # 用于匹配数字
                    if match1 and bool(match2) != True:  # 进行判断，如果包含参数名的字符，并且没有数字，则判定是参数名
                        n[num].append(1)
                    else:  # 否则则判定为参数值
                        n[num].append(0)
                num += 1  # 行计数加一
            detail = []  # 存放每一页的爬的表格信息
            count_r = []
            count_c = []
            for id, i in enumerate(n):  # 进行行统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_r.append(id)  # 记录标题行的行号
            n1 = np.array(n).T
            n = n1.tolist()
            for id, i in enumerate(n):  # 进行列统计
                if sum(i) > len(i) * 0.75:  # 统计每行中参数名的个数，如果大于一半行长度，则判定为标题行
                    count_c.append(id)
            if 0 in count_r and 0 not in count_c:  # 用第一行或者列做检验，如果如果在，首先证明，不可能是空的，如果第一行或者列在计数列表中，那么肯定不是行标题或者列标题
                print('检测出行表格，正在进行结构化数据抽取...')
                try:
                    for id, i in enumerate(count_r):  # i是标题行的行号
                        if i != count_r[-1]:  # 如果i不是最后一个标题行，这就意味着，i到i+1行之间的就是参数内容
                            for mm in m[i + 1:count_r[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail','a+',encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)

                except:
                    pass
            elif 0 in count_c and 0 not in count_r:  # 进行列标题的抽取。
                print('检测出列表格，正在进行结构化数据抽取...')
                try:
                    m = np.array(m).T
                    for id, i in enumerate(count_c):  # i是标题列的列号
                        if i != count_c[-1]:  # 如果i不是最后一个标题列，这就意味着，i到i+1列之间的就是参数内容
                            for mm in m[i + 1:count_c[id + 1]]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，m【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                        else:
                            for mm in m[i + 1:]:  # 遍历当前参数名行下面一行n[i+1],到下一参数名行之间的行count_r[id+1]
                                dic = {}  # 存放每个发动机的型号的，认为有多少个参数值行就是有多少个发动机型号
                                for idx, j in enumerate(m[i]):  # 开始遍历参数名行,idx是参数名在行中的下标，便于索引参数值行的内容
                                    dic.setdefault(j, mm[idx])  # 放进字典里，j是参数名，mm【idx】是按照参数名的下表索引参数值
                                detail.append(dic)
                    if len(detail) > 0:
                        with open('detail', 'a+', encoding='utf8') as f:
                            f.write(str(detail))
                            f.write('\n')
                        print(detail)

                except Exception as e:
                    print(e)
                    pass
            self.table.append(detail)
            item['detail'] = detail
            yield item
        # b = response.xpath('//*/@href')  # 定位到表格的行的父节点，即表各 这里对应的是一个页面一个列表的情况，
        # for i in b.extract():
        #     url = response.urljoin(i)
        #     print('链接',url)
        #     try:
        #         yield scrapy.Request(url=url, callback=self.parse2)  # 将当前页面抽取出的信息送去二级页面进行数据爬取
        #     except Exception as e:
        #         print('错误：', e)
        #         pass
        # c = response.xpath('//*[contains(text(),"下一页")]/@href').extract_first()
        # url = response.urljoin(c)
        # try:
        #     yield scrapy.Request(url=url, callback=self.parse1)
        #     print('下一页')
        # except Exception as e:
        #     print('错误：', e)
        #     pass