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
import G6, { Graph, GraphData, IG6GraphEvent } from '@antv/g6';
import { ZoomIn, ZoomOut, FullScreen, ArrowDown } from '@element-plus/icons-vue';
import { NODE_STYLE_MAP, EDGE_STYLE_MAP } from '@/types/graph';
import { NodeType, EdgeType } from '@/types/api';
import { ElMessage, ElMessageBox } from 'element-plus';

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
let graph: Graph | null = null;

// 小地图显示状态
const showMinimap = ref(true);

// 布局配置
const layouts = [
  { value: 'dagre', label: '层次布局' },
  { value: 'force', label: '力导向布局' },
  { value: 'concentric', label: '环形布局' },
  { value: 'radial', label: '辐射布局' },
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
      default: ['drag-canvas', 'zoom-canvas', 'drag-node', 'click-select']
    },
    layout: {
      type: 'dagre',
      rankdir: 'LR',
      align: 'UL',
      nodesep: 80,
      ranksep: 100
    },
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
      type: 'cubic',
      style: {
        stroke: '#aaa',
        lineWidth: 1,
        endArrow: {
          path: 'M 0,0 L 8,4 L 0,8 Z',
          fill: '#aaa'
        }
      },
      labelCfg: {
        autoRotate: true,
        style: {
          fill: '#666',
          fontSize: 10
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
        lineWidth: 2,
        stroke: '#1890ff'
      },
      selected: {
        lineWidth: 3,
        stroke: '#1abc9c'
      }
    },
    plugins: showMinimap.value ? [
      new G6.Minimap({
        container: minimapContainer.value!,
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
  graph.on('node:click', (e: IG6GraphEvent) => {
    const node = e.item?.getModel();
    if (node) {
      emit('nodeClick', node);
    }
  });
  
  // 边点击事件
  graph.on('edge:click', (e: IG6GraphEvent) => {
    const edge = e.item?.getModel();
    if (edge) {
      emit('edgeClick', edge);
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
const renderGraph = (data: { nodes: any[]; edges: any[] }) => {
  if (!graph) return;
  
  // 转换数据为G6格式
  const graphData: GraphData = {
    nodes: data.nodes.map(node => ({
      id: node.id,
      label: node.label,
      type: getNodeType(node.type),
      style: getNodeStyle(node.type),
      originalData: node
    })),
    edges: data.edges.map(edge => ({
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.label,
      style: getEdgeStyle(edge.type),
      originalData: edge
    }))
  };
  
  // 渲染图
  graph.data(graphData);
  graph.render();
  graph.fitView(20);
};

// 获取节点类型
const getNodeType = (type: NodeType): string => {
  switch (type) {
    case NodeType.DATABASE:
      return 'database-node';
    case NodeType.TABLE:
      return 'table-node';
    // 其他类型...
    default:
      return 'circle';
  }
};

// 获取节点样式
const getNodeStyle = (type: NodeType) => {
  return NODE_STYLE_MAP[type]?.style || {};
};

// 获取边样式
const getEdgeStyle = (type: EdgeType) => {
  return EDGE_STYLE_MAP[type]?.style || {};
};

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
  }
};

// 切换布局
const changeLayout = (layoutType: string) => {
  if (!graph) return;
  
  const layoutConfig = {
    type: layoutType,
    rankdir: 'LR',
    align: 'UL',
    nodesep: 80,
    ranksep: 100
  };
  
  // 更新当前布局
  currentLayout.value = layouts.find(layout => layout.value === layoutType) || layouts[0];
  
  // 应用新布局
  graph.updateLayout(layoutConfig);
  
  // 重新渲染
  graph.fitView(20);
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
watch(showMinimap, (newValue) => {
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
