import json
import xml.etree.ElementTree as ET
from svgpathtools import parse_path, Path
import numpy as np
import argparse
from os.path import splitext, exists, basename
from os import makedirs
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SVGtoPowerBIShapeMapConverter:
    """将SVG文件转换为符合Power BI Shape Map要求的GeoJSON格式"""

    def __init__(self, normalize_coords=True, max_coordinate=1000, simplify_tolerance=0.5):
        """
        初始化转换器

        参数:
            normalize_coords: 是否将坐标归一化到0-max_coordinate范围
            max_coordinate: 归一化后的最大坐标值（Power BI推荐0-1000）
            simplify_tolerance: 路径简化容差，减少坐标点数量
        """
        self.svg_width = 0
        self.svg_height = 0
        self.view_box = None
        self.features = []
        self.normalize_coords = normalize_coords
        self.max_coordinate = max_coordinate
        self.simplify_tolerance = simplify_tolerance
        self.region_counter = 0  # 用于自动生成区域ID

    def parse_svg(self, svg_path):
        """解析SVG文件并提取图形元素"""
        try:
            logger.info(f"开始解析SVG文件: {svg_path}")
            tree = ET.parse(svg_path)
            root = tree.getroot()

            # 获取SVG命名空间
            namespace = {"svg": "http://www.w3.org/2000/svg"}

            # 获取SVG尺寸信息
            self.svg_width = self._parse_length(root.get("width", 0))
            self.svg_height = self._parse_length(root.get("height", 0))
            self.view_box = root.get("viewBox")

            # 如果没有明确尺寸但有viewBox，使用viewBox的尺寸
            if self.view_box and (self.svg_width == 0 or self.svg_height == 0):
                vb = list(map(float, self.view_box.split()))
                self.svg_width = vb[2] - vb[0]
                self.svg_height = vb[3] - vb[1]

            logger.info(f"SVG尺寸: 宽度={self.svg_width}, 高度={self.svg_height}")

            # 提取所有路径和基本形状元素
            self._extract_elements(root, namespace)

            logger.info(f"解析完成，共提取 {len(self.features)} 个图形元素")
            return True
        except Exception as e:
            logger.error(f"解析SVG时出错: {str(e)}", exc_info=True)
            return False

    def _parse_length(self, length_str):
        """解析SVG长度值（处理可能的单位）"""
        if not length_str:
            return 0
        # 移除可能的单位（如px, em等）
        try:
            return float(''.join(filter(str.isdigit or (lambda c: c == '.'), length_str)))
        except ValueError:
            return 0

    def _extract_elements(self, root, namespace):
        """提取SVG中的所有图形元素，优先处理有ID的元素"""
        # 优先处理有ID的元素，这些通常是需要映射的区域
        elements_with_id = []
        elements_without_id = []

        # 收集所有元素
        for elem in root.findall(".//svg:path", namespace) + \
                    root.findall(".//svg:polygon", namespace) + \
                    root.findall(".//svg:rect", namespace) + \
                    root.findall(".//svg:circle", namespace) + \
                    root.findall(".//svg:ellipse", namespace):

            if elem.get("id"):
                elements_with_id.append(elem)
            else:
                elements_without_id.append(elem)

        # 先处理有ID的元素（这些是主要区域）
        logger.info(f"发现 {len(elements_with_id)} 个带ID的元素，{len(elements_without_id)} 个不带ID的元素")
        for elem in elements_with_id:
            self._process_element(elem)

        # 处理不带ID的元素（可选）
        for elem in elements_without_id:
            self._process_element(elem)

    def _process_element(self, element):
        """根据元素类型处理不同的SVG元素"""
        tag = element.tag.split('}')[-1]  # 移除命名空间前缀

        try:
            if tag == 'path':
                self._process_path(element)
            elif tag == 'rect':
                self._process_rect(element)
            elif tag == 'circle':
                self._process_circle(element)
            elif tag == 'ellipse':
                self._process_ellipse(element)
            elif tag == 'polygon':
                self._process_polygon(element)
            elif tag == 'polyline':
                # Power BI Shape Map主要关注多边形，折线可以忽略或转为线
                logger.warning("Polyline元素在Shape Map中可能无法正常显示，已跳过")
        except Exception as e:
            elem_id = element.get("id", "未知ID")
            logger.error(f"处理元素 {elem_id} 时出错: {str(e)}")

    def _process_path(self, path_element):
        """处理SVG路径元素，转换为Polygon"""
        d = path_element.get("d", "")
        elem_id = path_element.get("id", f"region_{self.region_counter}")

        if not d:
            logger.warning(f"路径元素 {elem_id} 没有路径数据，已跳过")
            return

        try:
            path = parse_path(d)
            coordinates = self._path_to_coordinates(path)

            # 简化路径，减少坐标点数量
            simplified_coords = self._simplify_coordinates(coordinates)

            # 确保多边形闭合
            if simplified_coords and simplified_coords[0] != simplified_coords[-1]:
                simplified_coords.append(simplified_coords[0])

            if len(simplified_coords) >= 4:  # 至少需要4个点（包括闭合点）
                feature = self._create_feature(
                    geometry_type="Polygon",
                    coordinates=[simplified_coords],
                    element=path_element,
                    elem_id=elem_id
                )
                self.features.append(feature)
                self.region_counter += 1
            else:
                logger.warning(f"路径元素 {elem_id} 坐标点太少，无法形成有效多边形")
        except Exception as e:
            logger.error(f"处理路径元素 {elem_id} 时出错: {str(e)}")

    def _process_rect(self, rect_element):
        """处理SVG矩形元素，转换为Polygon"""
        elem_id = rect_element.get("id", f"region_{self.region_counter}")

        try:
            x = float(rect_element.get("x", 0))
            y = float(rect_element.get("y", 0))
            width = float(rect_element.get("width", 0))
            height = float(rect_element.get("height", 0))

            # 忽略过小的矩形
            if width < 1 or height < 1:
                logger.warning(f"矩形元素 {elem_id} 尺寸过小，已跳过")
                return

            # 矩形的四个角（确保闭合）
            coordinates = [
                self._svg_to_geo_coords(x, y),
                self._svg_to_geo_coords(x + width, y),
                self._svg_to_geo_coords(x + width, y + height),
                self._svg_to_geo_coords(x, y + height),
                self._svg_to_geo_coords(x, y)  # 闭合多边形
            ]

            feature = self._create_feature(
                geometry_type="Polygon",
                coordinates=[coordinates],
                element=rect_element,
                elem_id=elem_id
            )
            self.features.append(feature)
            self.region_counter += 1
        except Exception as e:
            logger.error(f"处理矩形元素 {elem_id} 时出错: {str(e)}")

    def _process_circle(self, circle_element):
        """处理SVG圆形元素，转换为多边形近似"""
        elem_id = circle_element.get("id", f"region_{self.region_counter}")

        try:
            cx = float(circle_element.get("cx", 0))
            cy = float(circle_element.get("cy", 0))
            r = float(circle_element.get("r", 0))

            # 忽略过小的圆
            if r < 1:
                logger.warning(f"圆形元素 {elem_id} 半径过小，已跳过")
                return

            # 将圆转换为多边形近似（36个点足够平滑）
            coordinates = []
            num_points = 36
            for i in range(num_points + 1):
                angle = 2 * np.pi * i / num_points
                x = cx + r * np.cos(angle)
                y = cy + r * np.sin(angle)
                coordinates.append(self._svg_to_geo_coords(x, y))

            feature = self._create_feature(
                geometry_type="Polygon",
                coordinates=[coordinates],
                element=circle_element,
                elem_id=elem_id
            )
            self.features.append(feature)
            self.region_counter += 1
        except Exception as e:
            logger.error(f"处理圆形元素 {elem_id} 时出错: {str(e)}")

    def _process_ellipse(self, ellipse_element):
        """处理SVG椭圆元素，转换为多边形近似"""
        elem_id = ellipse_element.get("id", f"region_{self.region_counter}")

        try:
            cx = float(ellipse_element.get("cx", 0))
            cy = float(ellipse_element.get("cy", 0))
            rx = float(ellipse_element.get("rx", 0))
            ry = float(ellipse_element.get("ry", 0))

            # 忽略过小的椭圆
            if rx < 1 or ry < 1:
                logger.warning(f"椭圆元素 {elem_id} 尺寸过小，已跳过")
                return

            # 将椭圆转换为多边形近似
            coordinates = []
            num_points = 36
            for i in range(num_points + 1):
                angle = 2 * np.pi * i / num_points
                x = cx + rx * np.cos(angle)
                y = cy + ry * np.sin(angle)
                coordinates.append(self._svg_to_geo_coords(x, y))

            feature = self._create_feature(
                geometry_type="Polygon",
                coordinates=[coordinates],
                element=ellipse_element,
                elem_id=elem_id
            )
            self.features.append(feature)
            self.region_counter += 1
        except Exception as e:
            logger.error(f"处理椭圆元素 {elem_id} 时出错: {str(e)}")

    def _process_polygon(self, polygon_element):
        """处理SVG多边形元素"""
        elem_id = polygon_element.get("id", f"region_{self.region_counter}")

        try:
            points = polygon_element.get("points", "")
            if not points:
                logger.warning(f"多边形元素 {elem_id} 没有点数据，已跳过")
                return

            # 解析点坐标
            coords = []
            point_list = points.strip().split()
            for point in point_list:
                x, y = map(float, point.split(','))
                coords.append(self._svg_to_geo_coords(x, y))

            # 确保多边形闭合
            if coords and coords[0] != coords[-1]:
                coords.append(coords[0])

            # 简化坐标点
            simplified_coords = self._simplify_coordinates(coords)

            if len(simplified_coords) >= 4:  # 至少需要4个点（包括闭合点）
                feature = self._create_feature(
                    geometry_type="Polygon",
                    coordinates=[simplified_coords],
                    element=polygon_element,
                    elem_id=elem_id
                )
                self.features.append(feature)
                self.region_counter += 1
            else:
                logger.warning(f"多边形元素 {elem_id} 坐标点太少，无法形成有效多边形")
        except Exception as e:
            logger.error(f"处理多边形元素 {elem_id} 时出错: {str(e)}")

    def _path_to_coordinates(self, path, num_points=50):
        """将SVG路径转换为坐标列表"""
        coordinates = []
        length = path.length()

        if length == 0:
            # 处理零长度路径
            start = path.start
            coordinates.append(self._svg_to_geo_coords(start.real, start.imag))
            return coordinates

        # 沿路径均匀采样点
        for i in range(num_points + 1):
            position = i / num_points
            point = path.point(position)
            x, y = point.real, point.imag
            coordinates.append(self._svg_to_geo_coords(x, y))

        return coordinates

    def _svg_to_geo_coords(self, x, y):
        """
        将SVG坐标转换为适合Power BI Shape Map的坐标

        Power BI Shape Map推荐使用相对坐标，通常在0-1000范围内
        同时注意SVG的原点在左上角，而地图通常以左下角为原点
        """
        # 翻转y轴（SVG原点在左上角，地图通常在左下角）
        flipped_y = self.svg_height - y

        # 归一化坐标到0-max_coordinate范围
        if self.normalize_coords and self.svg_width > 0 and self.svg_height > 0:
            norm_x = (x / self.svg_width) * self.max_coordinate
            norm_y = (flipped_y / self.svg_height) * self.max_coordinate
            # 保留两位小数，避免精度问题
            return [round(norm_x, 2), round(norm_y, 2)]
        else:
            return [round(x, 2), round(flipped_y, 2)]

    def _simplify_coordinates(self, coordinates):
        """
        使用Douglas-Peucker算法简化坐标点，减少点数量

        参数:
            coordinates: 原始坐标列表
        返回:
            简化后的坐标列表
        """
        if len(coordinates) <= 3:
            return coordinates

        # Douglas-Peucker算法实现
        def distance(p, a, b):
            """计算点p到线段ab的距离"""
            if a == b:
                return np.hypot(p[0] - a[0], p[1] - a[1])
            return np.abs(np.cross(np.subtract(b, a), np.subtract(p, a))) / np.hypot(b[0] - a[0], b[1] - a[1])

        def simplify_recursive(coords, tol):
            """递归简化坐标"""
            if len(coords) <= 2:
                return coords

            a, b = coords[0], coords[-1]
            distances = [distance(p, a, b) for p in coords[1:-1]]
            max_dist = max(distances) if distances else 0
            max_index = distances.index(max_dist) + 1 if distances else 0

            if max_dist > tol:
                left = simplify_recursive(coords[:max_index + 1], tol)
                right = simplify_recursive(coords[max_index:], tol)
                return left[:-1] + right
            else:
                return [a, b]

        return simplify_recursive(coordinates, self.simplify_tolerance)

    def _create_feature(self, geometry_type, coordinates, element, elem_id):
        """创建符合Power BI要求的GeoJSON Feature"""
        # 提取元素属性，确保包含Power BI所需的映射字段
        props = self._get_element_properties(element, elem_id)

        return {
            "type": "Feature",
            "id": elem_id,  # 明确指定id，便于Power BI识别
            "geometry": {
                "type": geometry_type,
                "coordinates": coordinates
            },
            "properties": props
        }

    def _get_element_properties(self, element, elem_id):
        """
        提取元素属性，确保包含Power BI Shape Map所需的关键属性

        Power BI需要通过属性来关联数据，通常使用"id"和"name"字段
        """
        props = {
            # 确保有id和name字段，用于Power BI中的数据映射
            "id": elem_id,
            "name": element.get("title", element.get("name", elem_id))
        }

        # 添加其他有用属性
        for attr in ["class", "fill", "stroke", "stroke-width", "description"]:
            if element.get(attr) is not None:
                props[attr] = element.get(attr)

        return props

    def to_geojson(self, output_path=None):
        """将解析的SVG转换为符合Power BI要求的GeoJSON格式"""
        if not self.features:
            logger.warning("没有可转换的图形元素，无法生成GeoJSON")
            return None

        geojson = {
            "type": "FeatureCollection",
            "name": "PowerBIShapeMap",  # Power BI识别的名称
            "features": self.features
        }

        if output_path:
            # 确保输出目录存在
            output_dir = '/'.join(output_path.split('/')[:-1])
            if output_dir and not exists(output_dir):
                makedirs(output_dir)
                logger.info(f"创建输出目录: {output_dir}")

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, ensure_ascii=False, indent=2)
            logger.info(f"符合Power BI Shape Map的GeoJSON已保存到: {output_path}")

        return geojson


def main():
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='将SVG文件转换为符合Power BI Shape Map要求的GeoJSON格式')
    parser.add_argument('input', help='输入SVG文件路径')
    parser.add_argument('-o', '--output', help='输出GeoJSON文件路径，默认为输入文件路径替换扩展名')
    parser.add_argument('-n', '--no-normalize', action='store_true',
                        help='不进行坐标归一化，使用原始SVG坐标')
    parser.add_argument('-m', '--max-coord', type=int, default=1000,
                        help='坐标归一化的最大值，默认为1000（Power BI推荐）')
    parser.add_argument('-t', '--tolerance', type=float, default=0.5,
                        help='路径简化容差，值越大坐标点越少，默认为0.5')

    args = parser.parse_args()

    # 确定输出路径
    if not args.output:
        base, _ = splitext(args.input)
        args.output = f"{base}_powerbi.geojson"

    # 创建转换器实例并处理
    converter = SVGtoPowerBIShapeMapConverter(
        normalize_coords=not args.no_normalize,
        max_coordinate=args.max_coord,
        simplify_tolerance=args.tolerance
    )

    if converter.parse_svg(args.input):
        converter.to_geojson(args.output)
        logger.info("转换完成！可以在Power BI的Shape Map中使用此GeoJSON文件了")
        logger.info("提示：在Power BI中，请确保数据中的区域ID与GeoJSON中的'id'属性匹配")


if __name__ == "__main__":
    main()
