import json
from svgpathtools import svg2paths2
from shapely.geometry import Polygon, mapping

def svg_path_to_points(path, n_points=100):
    """
    将SVG Path转为坐标点列表
    path: svgpathtools Path 对象
    n_points: 分割路径的点数
    """
    points = []
    for i in range(n_points + 1):
        t = i / n_points
        point = path.point(t)
        points.append([point.real, point.imag])
    return points

def svg_to_geojson(svg_file, geojson_file):
    paths, attributes, svg_attr = svg2paths2(svg_file)
    features = []

    for path, attr in zip(paths, attributes):
        # 转换Path为点
        points = []
        if 'd' in attr:
            points = svg_path_to_points(path)
            # 尝试形成Polygon
            if points[0] != points[-1]:
                points.append(points[0])
            polygon = Polygon(points)
            feature = {
                "type": "Feature",
                "properties": attr,
                "geometry": mapping(polygon)
            }
            features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open(geojson_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    svg_file = r"C:\Users\13106\Desktop\tmp\SVG\GM_P1_P2_Mezzanine_new1.svg"       # 你的SVG文件
    geojson_file = "output1.geojson"  # 输出GeoJSON文件    svg_to_geojson(svg_file, geojson_file)
    print(f"GeoJSON已保存到 {geojson_file}")
