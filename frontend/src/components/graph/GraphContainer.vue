<template>
  <div class="graph-container" ref="graphContainer">
    <div class="graph-toolbar">
      <el-button-group>
        <el-tooltip content="放大" placement="top">
          <el-button @click="zoomIn">
            <el-icon><ZoomIn /></el-icon>
          </el-button>
        </el-tooltip>
        <el-tooltip content="缩小" placement="top">
          <el-button @click="zoomOut">
            <el-icon><ZoomOut /></el-icon>
          </el-button>
        </el-tooltip>
        <el-tooltip content="适应画布" placement="top">
          <el-button @click="fitView">
            <el-icon><FullScreen /></el-icon>
          </el-button>
        </el-tooltip>
      </el-button-group>
      
      <el-divider direction="vertical" />
      
      <el-tooltip content="切换布局" placement="top">
        <el-dropdown @command="changeLayout">
          <el-button>
            <span>{{ currentLayout.label }}</span>
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item v-for="layout in layouts" :key="layout.value" :command="layout.value">
                {{ layout.label }}
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </el-tooltip>
      
      <el-divider direction="vertical" />
      
      <el-tooltip content="导出图片" placement="top">
        <el-dropdown @command="exportGraph">
          <el-button>
            <span>导出</span>
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="png">PNG 图片</el-dropdown-item>
              <el-dropdown-item command="svg">SVG 矢量图</el-dropdown-item>
              <el-dropdown-item command="json">JSON 数据</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </el-tooltip>
      
      <div class="spacer"></div>
      
      <el-switch
        v-model="showMinimap"
        active-text="显示小地图"
        inactive-text=""
      />
    </div>
    
    <div class="graph-wrapper" ref="graphWrapper"></div>
    
    <div v-if="showMinimap" class="graph-minimap" ref="minimapContainer"></div>
    
    <div v-if="loading" class="graph-loading">
      <el-spinner size="large" />
      <p>加载中...</p>
    </div>
    
    <div v-if="error" class="graph-error">
      <el-alert
        title="加载图数据失败"
        type="error"
        :description="error"
        show-icon
        :closable="false"
      />
      <el-button type="primary" @click="retry" class="retry-button">重试</el-button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue';
import G6 from '@antv/g6';
import { ZoomIn, ZoomOut, FullScreen, ArrowDown } from '@element-plus/icons-vue';
import { NODE_STYLE_MAP, EDGE_STYLE_MAP } from '@/types/graph';
import { NodeType, EdgeType } from '@/types/api';
import { ElMessage } from 'element-plus';

// 定义属性
const props = defineProps<{
  data?: {
    nodes: any[];
    edges: any[];
  };
  loading?: boolean;
  error?: string;
}>();

// 定义事件
const emit = defineEmits<{
  (e: 'nodeClick', node: any): void;
  (e: 'edgeClick', edge: any): void;
  (e: 'retry'): void;
}>();

// 图容器引用
const graphContainer = ref<HTMLElement | null>(null);
const graphWrapper = ref<HTMLElement | null>(null);
const minimapContainer = ref<HTMLElement | null>(null);

// 图实例
let graph: any = null;

// 小地图显示状态
const showMinimap = ref(true);

// 布局配置 - 专注于层次布局变体
const layouts = [
  { value: 'dagre', label: '层次布局（推荐）' },
  { value: 'force', label: '力导向布局' },
  { value: 'concentric', label: '环形布局' },
  { value: 'grid', label: '网格布局' }
];

// 当前布局
const currentLayout = ref(layouts[0]);

// 注册自定义节点
const registerCustomNodes = () => {
  // 数据库节点
  G6.registerNode('database-node', {
    draw(cfg, group) {
      const style = NODE_STYLE_MAP[NodeType.DATABASE];
      const size = style.size as number;
      const keyShape = group!.addShape('circle', {
        attrs: {
          x: 0,
          y: 0,
          r: size / 2,
          fill: style.color,
          stroke: style.style?.stroke || '#000',
          lineWidth: style.style?.lineWidth || 1,
          cursor: 'pointer'
        },
        name: 'database-keyShape'
      });
      
      // 添加图标
      group!.addShape('text', {
        attrs: {
          x: 0,
          y: 0,
          text: '🗄️',
          fontSize: 16,
          textAlign: 'center',
          textBaseline: 'middle',
          fill: '#fff',
          cursor: 'pointer'
        },
        name: 'database-icon'
      });
      
      // 添加标签
      if (cfg?.label) {
        group!.addShape('text', {
          attrs: {
            x: 0,
            y: size / 2 + 10,
            text: cfg.label,
            fontSize: 12,
            textAlign: 'center',
            textBaseline: 'top',
            fill: '#333',
            cursor: 'pointer'
          },
          name: 'database-label'
        });
      }
      
      return keyShape;
    }
  });
  
  // 表节点
  G6.registerNode('table-node', {
    draw(cfg, group) {
      const style = NODE_STYLE_MAP[NodeType.TABLE];
      const width = (style.size as number[])[0];
      const height = (style.size as number[])[1];
      const keyShape = group!.addShape('rect', {
        attrs: {
          x: -width / 2,
          y: -height / 2,
          width,
          height,
          fill: style.color,
          stroke: style.style?.stroke || '#000',
          lineWidth: style.style?.lineWidth || 1,
          radius: style.style?.radius || 0,
          cursor: 'pointer'
        },
        name: 'table-keyShape'
      });
      
      // 添加图标
      group!.addShape('text', {
        attrs: {
          x: 0,
          y: 0,
          text: '📋',
          fontSize: 14,
          textAlign: 'center',
          textBaseline: 'middle',
          fill: '#fff',
          cursor: 'pointer'
        },
        name: 'table-icon'
      });
      
      // 添加标签
      if (cfg?.label) {
        group!.addShape('text', {
          attrs: {
            x: 0,
            y: height / 2 + 10,
            text: cfg.label,
            fontSize: 12,
            textAlign: 'center',
            textBaseline: 'top',
            fill: '#333',
            cursor: 'pointer'
          },
          name: 'table-label'
        });
      }
      
      return keyShape;
    }
  });
  
  // 其他节点类型...
};

// 注册自定义血缘关系边，仅用于data_flow
G6.registerEdge('custom-data-flow', {
  draw(cfg, group) {
    const { startPoint, endPoint } = cfg;
    // 控制点横向偏移，纵向与起点一致，形成优雅的弯曲
    const controlX = startPoint.x - Math.abs(endPoint.x - startPoint.x) * 0.4;
    const path = [
      ['M', startPoint.x, startPoint.y],
      ['Q', controlX, startPoint.y, endPoint.x, endPoint.y]
    ];
    const shape = group.addShape('path', {
      attrs: {
        path,
        stroke: '#ff4d4f',
        lineWidth: 3
        // 不设置endArrow，去除箭头
      },
      name: 'custom-data-flow-path'
    });
    return shape;
  }
}, 'quadratic');

// 初始化图
const initGraph = () => {
  if (!graphWrapper.value) return;
  
  // 注册自定义节点
  registerCustomNodes();
  
  // 创建图实例
  graph = new G6.Graph({
    container: graphWrapper.value,
    width: graphWrapper.value.clientWidth,
    height: graphWrapper.value.clientHeight,
    modes: {
      default: ['drag-canvas', 'zoom-canvas', 'drag-node']
    },
    layout: {
      type: 'dagre',
      rankdir: 'LR',        // 从左到右布局
      align: 'UL',          // 上对齐
      nodesep: 60,          // 同层节点间距
      ranksep: 150,         // 层间距离
      controlPoints: true,  // 启用控制点
    },
    fitView: true,  // 启用自动适应
    fitViewPadding: [50, 50, 50, 50],
    defaultNode: {
      type: 'circle',
      size: 30,
      style: {
        fill: '#5B8FF9',
        stroke: '#3057e3',
        lineWidth: 2
      },
      labelCfg: {
        position: 'bottom',
        offset: 10,
        style: {
          fill: '#333',
          fontSize: 12
        }
      }
    },
    defaultEdge: {
      type: 'quadratic',     // 使用二次贝塞尔曲线，血缘关系更美观
      style: {
        stroke: '#999',
        lineWidth: 1.5,
        cursor: 'pointer',
        // 只为非data_flow类型设置endArrow
        // endArrow: {
        //   path: 'M 0,0 L 8,4 L 0,8 Z',
        //   fill: '#999'
        // }
      },
      // 设置连接点位置：从源节点右侧连接到目标节点右侧
      sourceAnchor: 1,       // 源节点右侧连接点
      targetAnchor: 0,       // 目标节点左侧连接点
      labelCfg: {
        autoRotate: false,   // 不自动旋转标签
        style: {
          fill: '#333',
          fontSize: 11,
          fontWeight: 500,
          background: {
            fill: 'rgba(255,255,255,0.9)',
            stroke: '#ddd',
            padding: [1, 3, 1, 3],
            radius: 2
          }
        }
      }
    },
    nodeStateStyles: {
      hover: {
        lineWidth: 3,
        shadowColor: '#1890ff',
        shadowBlur: 10
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c',
        shadowColor: '#1abc9c',
        shadowBlur: 15
      }
    },
    edgeStateStyles: {
      hover: {
        lineWidth: 3,
        stroke: '#1890ff',
        shadowColor: '#1890ff',
        shadowBlur: 8,
        cursor: 'pointer'
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c',
        shadowColor: '#1abc9c',
        shadowBlur: 12,
        cursor: 'pointer'
      }
    },
    plugins: showMinimap.value ? [
      new G6.Minimap({
        container: minimapContainer.value as HTMLDivElement,
        size: [150, 100]
      })
    ] : []
  });
  
  // 绑定事件
  bindEvents();
  
  // 如果有数据，渲染图
  if (props.data) {
    renderGraph(props.data);
  }
};

// 绑定事件
const bindEvents = () => {
  if (!graph) return;
  
  // 节点点击事件
  graph.on('node:click', (e: any) => {
    const node = e.item?.getModel();
    if (node) {
      // 清除所有选中状态
      graph.getNodes().forEach((nodeItem: any) => {
        graph.clearItemStates(nodeItem, ['selected']);
      });
      graph.getEdges().forEach((edgeItem: any) => {
        graph.clearItemStates(edgeItem, ['selected']);
      });
      
      // 设置当前节点为选中状态
      if (e.item) {
        graph.setItemState(e.item, 'selected', true);
      }
      
      emit('nodeClick', node);
    }
  });
  
  // 边点击事件
  graph.on('edge:click', (e: any) => {
    const edge = e.item?.getModel();
    if (edge) {
      // 清除所有选中状态
      graph.getNodes().forEach((nodeItem: any) => {
        graph.clearItemStates(nodeItem, ['selected']);
      });
      graph.getEdges().forEach((edgeItem: any) => {
        graph.clearItemStates(edgeItem, ['selected']);
      });
      
      // 设置当前边为选中状态
      if (e.item) {
        graph.setItemState(e.item, 'selected', true);
      }
      
      emit('edgeClick', edge);
    }
  });
  
  // 边悬停事件
  graph.on('edge:mouseenter', (e: any) => {
    if (e.item) {
      graph.setItemState(e.item, 'hover', true);
    }
  });
  
  graph.on('edge:mouseleave', (e: any) => {
    if (e.item) {
      graph.setItemState(e.item, 'hover', false);
    }
  });
  
  // 窗口大小改变事件
  const handleResize = () => {
    if (graph && graphWrapper.value) {
      graph.changeSize(graphWrapper.value.clientWidth, graphWrapper.value.clientHeight);
      graph.fitView(20);
    }
  };
  
  window.addEventListener('resize', handleResize);
  
  // 组件卸载时移除事件监听
  onUnmounted(() => {
    window.removeEventListener('resize', handleResize);
  });
};

  // 渲染图
const renderGraph = (data: { nodes: any[]; edges: any[], focusId?: string }) => {
  console.log('开始渲染图形:', data);
  if (!graph) {
    console.error('图实例不存在，无法渲染');
    return;
  }
  
  if (!data || !data.nodes || !data.edges) {
    console.error('数据不完整，无法渲染:', data);
    return;
  }
  
  console.log('原始数据:', data);
  console.log('节点数量:', data.nodes.length);
  console.log('边数量:', data.edges.length);
  
  try {
      // 转换数据为G6格式，让dagre布局自动计算位置
  const { processedNodes, processedEdges } = processGraphData(data);
    
    const graphData = {
      nodes: processedNodes,
      edges: processedEdges
    };
    
    console.log('转换后的G6格式数据:', graphData);
    
        // 渲染图
    graph.data(graphData);
    graph.render();
    
    // 延迟执行适应视图
    setTimeout(() => {
      if (graph) {
        // 适应视图，确保所有节点都可见
        graph.fitView(20);
        // 居中聚焦被筛选对象
        if (data.focusId) {
          graph.focusItem(data.focusId, true, {
            easing: 'easeCubic',
            duration: 600
          });
        } else if (processedNodes.length > 0) {
          // 默认聚焦第一个节点
          graph.focusItem(processedNodes[0].id, true, {
            easing: 'easeCubic',
            duration: 600
          });
        }
        console.log('图形渲染完成，使用dagre层级布局');
      }
    }, 100);
  } catch (error) {
    console.error('渲染图形时发生错误:', error);
  }
};

// 获取节点类型
const getNodeType = (type: string): string => {
  // 处理后端返回的节点类型字符串
  switch (type.toLowerCase()) {
    case 'database':
      return 'database-node';
    case 'table':
      return 'table-node';
    case 'view':
      return 'table-node'; // 视图使用表节点样式
    case 'column':
      return 'circle';
    case 'schema':
      return 'circle';
    case 'sqlpattern':
    case 'sql_pattern':
      return 'circle';
    case 'function':
      return 'circle';
    default:
      console.warn('未知节点类型:', type, '使用默认circle');
      return 'circle';
  }
};

// 获取节点样式
const getNodeStyle = (type: string) => {
  // 转换后端类型到前端NodeType枚举
  let nodeType: NodeType;
  switch (type.toLowerCase()) {
    case 'database':
      nodeType = NodeType.DATABASE;
      break;
    case 'table':
      nodeType = NodeType.TABLE;
      break;
    case 'view':
      nodeType = NodeType.VIEW;
      break;
    case 'column':
      nodeType = NodeType.COLUMN;
      break;
    case 'schema':
      nodeType = NodeType.SCHEMA;
      break;
    case 'sqlpattern':
    case 'sql_pattern':
      nodeType = NodeType.SQL_PATTERN;
      break;
    case 'function':
      nodeType = NodeType.FUNCTION;
      break;
    default:
      nodeType = NodeType.TABLE; // 默认为表
  }
  return NODE_STYLE_MAP[nodeType]?.style || {};
};

// 获取边样式
const getEdgeStyle = (type: string) => {
  // 转换后端类型到前端EdgeType枚举
  let edgeType: EdgeType;
  let customStyle: any = {};
  
  switch (type.toLowerCase()) {
    case 'has_schema':
    case 'has_object':
    case 'has_column':
    case 'contains':
      edgeType = EdgeType.CONTAINS;
      // 结构关系使用灰色细线，不要箭头
      customStyle = {
        stroke: '#bbb',
        lineWidth: 1,
        lineDash: [2, 2],
        endArrow: false  // 明确禁用箭头
      };
      break;
    case 'reads_from':
    case 'reads':
      edgeType = EdgeType.READS;
      break;
    case 'writes_to':
    case 'writes':
      edgeType = EdgeType.WRITES;
      // 写入关系不再保留箭头
      customStyle = {
        stroke: '#52c41a',
        lineWidth: 2,
        endArrow: false // 明确禁用箭头
      };
      break;
    case 'data_flow':
      edgeType = EdgeType.DATA_FLOW;
      // 数据流关系不设置箭头
      customStyle = {
        stroke: '#ff4d4f',
        lineWidth: 3,
        endArrow: false  // 明确禁用箭头
      };
      break;
    case 'generates':
    case 'generates_flow':
      edgeType = EdgeType.GENERATES_FLOW;
      break;
    case 'references':
      edgeType = EdgeType.REFERENCES;
      break;
    default:
      edgeType = EdgeType.DEPENDS_ON;
  }
  
  // 获取默认样式，但如果customStyle明确设置了endArrow为false，则移除默认的endArrow
  const defaultStyle = EDGE_STYLE_MAP[edgeType]?.style || {};
  const mergedStyle = { ...defaultStyle, ...customStyle };
  
  // 如果customStyle明确禁用了箭头，确保最终样式中没有箭头
  if (customStyle.endArrow === false) {
    delete mergedStyle.endArrow;
  }
  
  return mergedStyle;
};

// 处理图数据 - 使用dagre布局，血缘关系：目标在左，源在右
const processGraphData = (data: { nodes: any[]; edges: any[] }) => {
  console.log('开始处理图数据 - 血缘关系布局：目标在左，源在右');

  // 1. 统计所有血缘源端id（data_flow/writes_to/writes的source端）
  const dataFlowSourceIds = new Set(
    data.edges
      .filter(e => ['data_flow', 'writes_to', 'writes'].includes((e.type || '').toLowerCase()))
      .map(e => e.source)
  );

  // 2. 计算节点分层
  const processedNodes = data.nodes.map((node) => {
    const nodeType = getNodeType(node.type);
    const layerInfo = calculateNodeLayer(node, dataFlowSourceIds);
    return {
      id: node.id,
      label: node.label,
      type: nodeType,
      style: getNodeStyle(node.type),
      originalData: node,
      layer: layerInfo.layer,
      rank: layerInfo.rank,
      weight: layerInfo.weight
    };
  });

  // 3. 转换边数据，根据类型设置不同的连接方式
  const processedEdges = data.edges.map(edge => {
    const edgeStyle = getEdgeStyle(edge.type);
    if (edgeStyle && edgeStyle.endArrow === false) {
      delete edgeStyle.endArrow;
    }
    const edgeType = edge.type?.toLowerCase() || '';
    let edgeConfig: any = {
      id: edge.id,
      label: edge.label,
      style: edgeStyle,
      originalData: edge
    };
    if (edgeType === 'data_flow') {
      edgeConfig.type = 'custom-data-flow';
      edgeConfig.source = edge.target;
      edgeConfig.target = edge.source;
      edgeConfig.sourceAnchor = 1;
      edgeConfig.targetAnchor = 3;
    } else if (["has_schema", "has_object", "has_column"].includes(edgeType)) {
      edgeConfig.type = 'polyline';
      edgeConfig.source = edge.source;
      edgeConfig.target = edge.target;
      edgeConfig.sourceAnchor = 1;
      edgeConfig.targetAnchor = 3;
    } else {
      edgeConfig.type = 'quadratic';
      edgeConfig.source = edge.source;
      edgeConfig.target = edge.target;
    }
    return edgeConfig;
  });

  console.log('图数据处理完成 - 使用dagre自动布局', {
    nodes: processedNodes.length,
    edges: processedEdges.length
  });

  return { processedNodes, processedEdges };
};

// 计算节点层级 - 元数据链路始终分层，血缘源端layer提升但不影响结构链路
const calculateNodeLayer = (node: any, dataFlowSourceIds: Set<string>): { layer: number, rank: number, weight: number } => {
  const nodeType = node.type?.toLowerCase() || '';
  let layer = 2; // 默认表层
  let rank = 0;
  let weight = 0;

  if (nodeType === 'database') {
    layer = 0;
    weight = 10;
  } else if (nodeType === 'schema') {
    layer = 1;
    weight = 20;
  } else if (nodeType === 'table') {
    layer = 2;
    weight = 100;
  } else if (nodeType === 'column') {
    layer = 3;
    weight = 110;
  } else if (nodeType === 'sqlpattern' || nodeType === 'sql_pattern') {
    // sqlpattern始终与表同级，体现加工方式
    layer = 2;
    weight = 80;
  } else {
    layer = 2;
    weight = 40;
  }

  // 只有非sqlpattern的血缘源端才提升layer
  if (dataFlowSourceIds.has(node.id) && !(nodeType === 'sqlpattern' || nodeType === 'sql_pattern')) {
    layer = Math.max(layer, 5);
    weight += 1000;
  }
  return { layer, rank, weight };
};

// 移除手动坐标计算，使用dagre自动布局

// 放大
const zoomIn = () => {
  if (graph) {
    const zoom = graph.getZoom();
    const maxZoom = 5;
    if (zoom < maxZoom) {
      graph.zoomTo(zoom * 1.1);
    }
  }
};

// 缩小
const zoomOut = () => {
  if (graph) {
    const zoom = graph.getZoom();
    const minZoom = 0.1;
    if (zoom > minZoom) {
      graph.zoomTo(zoom * 0.9);
    }
  }
};

// 适应画布
const fitView = () => {
  if (graph) {
    graph.fitView(20);
    graph.fitCenter();
  }
};

// 切换布局
const changeLayout = (layoutType: string) => {
  if (!graph) return;
  
  let layoutConfig: any = {};
  
  switch (layoutType) {
    case 'dagre':
      layoutConfig = {
        type: 'dagre',
        rankdir: 'LR',        // 从左到右
        align: 'UL',          // 上对齐
        nodesep: 60,          // 同层节点间距
        ranksep: 200,         // 增大层间距离
        controlPoints: true,
        // 使用自定义排序函数
        sortMethod: (nodeA: any, nodeB: any) => {
          const weightA = nodeA.layoutWeight || 0;
          const weightB = nodeB.layoutWeight || 0;
          console.log(`排序比较: ${nodeA.label}(${weightA}) vs ${nodeB.label}(${weightB})`);
          return weightA - weightB;
        },
        // 强制分层
        ranker: 'tight-tree'
      };
      break;
    case 'force':
      layoutConfig = {
        type: 'force',
        center: [400, 300],
        linkDistance: 100,
        nodeStrength: -200,
        preventOverlap: true,
        nodeSize: 30
      };
      break;
    case 'concentric':
      layoutConfig = {
        type: 'concentric',
        minNodeSpacing: 50,
        preventOverlap: true,
        nodeSize: 30
      };
      break;
    case 'grid':
      layoutConfig = {
        type: 'grid',
        cols: 5,
        rows: 5,
        sortBy: 'degree'
      };
      break;
    default:
      layoutConfig = {
        type: 'dagre',
        rankdir: 'LR',
        align: 'UL',
        nodesep: 40,
        ranksep: 120
      };
  }
  
  // 更新当前布局
  currentLayout.value = layouts.find(layout => layout.value === layoutType) || layouts[0];
  
  // 应用新布局
  graph.updateLayout(layoutConfig);
  
  // 重新渲染
  setTimeout(() => {
    graph.fitView(20);
    graph.fitCenter();
  }, 100);
};

// 导出图
const exportGraph = (type: string) => {
  if (!graph) return;
  
  switch (type) {
    case 'png':
      graph.downloadFullImage('lineage-graph', 'image/png', {
        backgroundColor: '#fff',
        padding: [20, 20, 20, 20]
      });
      break;
    case 'svg':
      // G6 目前不直接支持导出 SVG，这里是一个简化的实现
      ElMessage.warning('SVG导出功能正在开发中');
      break;
    case 'json':
      const data = graph.save();
      const dataStr = JSON.stringify(data, null, 2);
      const blob = new Blob([dataStr], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'lineage-graph.json';
      link.click();
      URL.revokeObjectURL(url);
      break;
  }
};

// 重试
const retry = () => {
  emit('retry');
};

// 监听数据变化
watch(() => props.data, (newData) => {
  if (newData && graph) {
    renderGraph(newData);
  }
}, { deep: true });

// 监听小地图显示状态变化
watch(showMinimap, () => {
  if (graph) {
    // 销毁旧图实例
    graph.destroy();
    
    // 重新初始化图
    nextTick(() => {
      initGraph();
    });
  }
});

// 组件挂载时初始化图
onMounted(() => {
  initGraph();
});

// 组件卸载时销毁图
onUnmounted(() => {
  if (graph) {
    graph.destroy();
    graph = null;
  }
});
</script>

<style lang="scss" scoped>
.graph-container {
  position: relative;
  width: 100%;
  height: 100%;
  background-color: #f9f9f9;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  overflow: hidden;
  
  .graph-toolbar {
    position: absolute;
    top: 10px;
    left: 10px;
    z-index: 10;
    display: flex;
    align-items: center;
    background-color: rgba(255, 255, 255, 0.9);
    border-radius: 4px;
    padding: 5px 10px;
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
    
    .spacer {
      flex: 1;
    }
  }
  
  .graph-wrapper {
    width: 100%;
    height: 100%;
  }
  
  .graph-minimap {
    position: absolute;
    bottom: 10px;
    right: 10px;
    z-index: 10;
    border: 1px solid var(--border-color);
    border-radius: 4px;
    overflow: hidden;
    background-color: rgba(255, 255, 255, 0.9);
    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
  }
  
  .graph-loading,
  .graph-error {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    background-color: rgba(255, 255, 255, 0.8);
    z-index: 20;
    
    p {
      margin-top: 10px;
      color: #666;
    }
  }
  
  .graph-error {
    .retry-button {
      margin-top: 20px;
    }
  }
}
</style>
