# import os
# import json
# from jinja2 import Environment, FileSystemLoader, select_autoescape
# import sqlite3 as sl
# from time import time_ns
#
#
# # import dominate
# # from dominate import tags as dom_tags
#
# def sizeof_fmt(num, suffix='B'):
#     for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
#         if abs(num) < 1024.0:
#             return "%3.1f%s%s" % (num, unit, suffix)
#         num /= 1024.0
#     return "%.1f%s%s" % (num, 'Yi', suffix)
#
#
# def make_report(result_folder_list):
#     report = {}
#     for folder in result_folder_list:
#         file_list = os.listdir(folder)
#
#         report[folder] = {}
#         with open(os.path.join(folder, 'results.json')) as f:
#             report[folder]['results'] = json.loads(f.read())
#         with open(os.path.join(folder, 'result.log')) as f:
#             report[folder]['log'] = f.read()
#         report[folder]['results_npz'] = {'size: ':
#           sizeof_fmt(os.stat(os.path.join(folder, 'results.npz')).st_size)}
#     return report
#
#
# # def make_result_report(result_folder_list):
# #     doc = dominate.document(title='QM results report')
# #
# #     with doc.head:
# #         dom_tags.link(rel='stylesheet', href='style.css')
# #         # dom_tags.script(type='text/javascript', src='script.js')
# #
# #     with doc:
# #         with dom_tags.div(id='header').add(dom_tags.ol()):
# #             dom_tags.h1('QM Results report')
# #
# #         with dom_tags.div():
# #             dom_tags.attr(cls='body')
# #
# #             for folder in result_folder_list:
# #                 with dom_tags.div(id=folder):
# #                     dom_tags.p(f"{folder}")
# #                     for file_name in os.listdir(folder):
# #                         with dom_tags.div(id=file_name):
# #                             if file_name.split('.')[1] == 'npz':
# #                                 dom_tags.p(file_name)
# #                             else:
# #                                 with open(os.path.join(folder, file_name), 'r') as f:
# #                                     dom_tags.p(f.read())
# #
# #     with open('report.html', 'w') as report_html:
# #         report_html.write(doc.__str__())
#
#
# def write_res_to_db(db_name):
#     con = sl.connect(db_name)
#
#     with con:
#         g_id = time_ns()
#         n_id = time_ns()
#         r_id = time_ns()
#         con.execute("""
#                     INSERT INTO Graphs (graph_id,graph_script)
#                     VALUES (?,1);
#                     """, (g_id,))
#         con.execute("""
#                     INSERT INTO Nodes (graph_id,node_id)
#                     VALUES (?,?);
#                     """, (g_id, n_id))
#         con.execute("""
#                     INSERT INTO NodeResults (
#                       graph_id,node_id,result_id,user_id,start_time,end_time,name,val)
#                     VALUES (?,?,?,'Gal',1,2,'this',12);
#
#
#                     """, (g_id, n_id, r_id))
#         con.execute(f"""
#                         INSERT INTO Metadata (graph_id,node_id,data_id,name,val)
#                         VALUES (?,?,1,'V1',1)
#                         """, (g_id, n_id))
#
#
# def get_results_in_path(path):
#     folder_list = [x[0] for x in os.walk(path)]
#     res_folder_list = []
#     for folder in folder_list:
#         if os.path.exists(os.path.join(folder, 'result.log')):
#             res_folder_list.append(folder)
#             # print('results folder found!')
#     return res_folder_list
#
#
# def get_results_by_param(res_folder_list, param, val):
#     filterd_report = {}
#     report = make_report(res_folder_list)
#     for key in report.keys():
#         if report[key]['results'][param] == val:
#             filterd_report[key] = report[key]
#     return filterd_report
#
#
# def make_web_report(report):
#     env = Environment(
#         loader=FileSystemLoader(os.path.join('', 'report_templates')),
#         autoescape=select_autoescape(['html', 'xml'])
#     )
#     template = env.get_template('template.html')
#
#     return template.render(report=report)