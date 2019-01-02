from pyecharts import GeoLines, Style
import pandas as pd
import os


def gen_render_html(nodes_path, edges_path, output_path=r'../results/energy/figure/2000-2017/city_coop.html'):
    # 读取节点和边
    nodes = pd.read_csv(nodes_path, delimiter=';')
    edges = pd.read_csv(edges_path, delimiter=';')

    # 设置生成echarts地理流向图的配置
    geocode = {nodes.iloc[i]['name']: [nodes.iloc[i]['Longitude'], nodes.iloc[i]['Latitude']]
               for i in range(len(nodes))}
    style = Style(title_color= "#fff",title_pos = "center", width = 2400,height = 1200,background_color = "white")
    edge_data = [[edges.iloc[i]['source'], edges.iloc[i]['target'], edges.iloc[i]['weight']]
                 for i in range(len(edges))]
    style_geo = style.add(line_opacity=0.2, line_color='#2980b9')

    # 渲染
    geolines = GeoLines('城市合作网络', **style.init_style)
    geolines.add("", edge_data, maptype='world', geo_cities_coords=geocode, is_geo_effect_show=False, is_roam=False,
                 symbol_size=0, geo_normal_color='#dcdcdc', **style_geo)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    geolines.render(output_path)

    return

if __name__ == '__main__':
    NODES_PATH = r'C:\Users\Tom\PycharmProjects\MET\results\energy\figure\2000-2017\2000-2017-city_nodes.csv'
    EDGES_PATH = r'C:\Users\Tom\PycharmProjects\MET\results\energy\figure\2000-2017\2000-2017-city_edges.csv'

    print('开始渲染地理流向图')
    gen_render_html(NODES_PATH, EDGES_PATH)
    print('生成完成了')


