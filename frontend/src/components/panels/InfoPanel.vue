<template>
  <div class="info-panel" :class="{ 'is-collapsed': isCollapsed }">
    <div class="panel-header">
      <h3>{{ title }}</h3>
      <div class="panel-actions">
        <el-button circle size="small" @click="toggleCollapse">
          <el-icon>
            <ArrowRight v-if="isCollapsed" />
            <ArrowLeft v-else />
          </el-icon>
        </el-button>
      </div>
    </div>
    
    <div v-if="!isCollapsed" class="panel-content">
      <div v-if="loading" class="panel-loading">
        <el-spinner size="small" />
        <p>加载中...</p>
      </div>
      
      <div v-else-if="error" class="panel-error">
        <el-alert
          title="加载详情失败"
          type="error"
          :description="error"
          show-icon
          :closable="false"
        />
      </div>
      
      <template v-else-if="selectedItem">
        <!-- 节点详情 -->
        <template v-if="itemType === 'node'">
          <div class="item-header">
            <div class="item-icon" :style="{ backgroundColor: getNodeColor(selectedItem.type) }">
              <span>{{ getNodeIcon(selectedItem.type) }}</span>
            </div>
            <div class="item-title">
              <h4>{{ selectedItem.label }}</h4>
              <span class="item-type">{{ getNodeTypeLabel(selectedItem.type) }}</span>
            </div>
          </div>
          
          <el-divider />
          
          <el-descriptions :column="1" border>
            <el-descriptions-item label="ID">
              {{ selectedItem.id }}
            </el-descriptions-item>
            <el-descriptions-item v-if="selectedItem.fqn" label="全限定名">
              {{ selectedItem.fqn }}
            </el-descriptions-item>
            <el-descriptions-item label="类型">
              {{ getNodeTypeLabel(selectedItem.type) }}
            </el-descriptions-item>
          </el-descriptions>
          
          <template v-if="Object.keys(selectedItem.properties || {}).length > 0">
            <h4 class="section-title">属性</h4>
            <el-descriptions :column="1" border>
              <el-descriptions-item v-for="(value, key) in selectedItem.properties" :key="key" :label="formatPropertyKey(key)">
                {{ formatPropertyValue(value) }}
              </el-descriptions-item>
            </el-descriptions>
          </template>
          
          <div class="actions-container">
            <el-button type="primary" @click="viewDetails">查看详情</el-button>
            <el-button @click="expandNeighbors">展开邻居</el-button>
          </div>
        </template>
        
        <!-- 边详情 -->
        <template v-else-if="itemType === 'edge'">
          <div class="item-header">
            <div class="item-icon" :style="{ backgroundColor: getEdgeColor(selectedItem.type) }">
              <el-icon><Connection /></el-icon>
            </div>
            <div class="item-title">
              <h4>{{ selectedItem.label }}</h4>
              <span class="item-type">{{ getEdgeTypeLabel(selectedItem.type) }}</span>
            </div>
          </div>
          
          <el-divider />
          
          <el-descriptions :column="1" border>
            <el-descriptions-item label="ID">
              {{ selectedItem.id }}
            </el-descriptions-item>
            <el-descriptions-item label="源节点">
              {{ getSourceNodeLabel() }}
            </el-descriptions-item>
            <el-descriptions-item label="目标节点">
              {{ getTargetNodeLabel() }}
            </el-descriptions-item>
            <el-descriptions-item label="关系类型">
              {{ getEdgeTypeLabel(selectedItem.type) }}
            </el-descriptions-item>
          </el-descriptions>
          
          <template v-if="Object.keys(selectedItem.properties || {}).length > 0">
            <h4 class="section-title">属性</h4>
            <el-descriptions :column="1" border>
              <el-descriptions-item v-for="(value, key) in selectedItem.properties" :key="key" :label="formatPropertyKey(key)">
                {{ formatPropertyValue(value) }}
              </el-descriptions-item>
            </el-descriptions>
          </template>
        </template>
        
        <!-- 无选中项 -->
        <template v-else>
          <div class="empty-state">
            <el-icon><InfoFilled /></el-icon>
            <p>请在图中选择一个节点或边查看详情</p>
          </div>
        </template>
      </template>
      
      <!-- 无选中项 -->
      <template v-else>
        <div class="empty-state">
          <el-icon><InfoFilled /></el-icon>
          <p>请在图中选择一个节点或边查看详情</p>
        </div>
      </template>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed } from 'vue';
import { useRouter } from 'vue-router';
import { ArrowLeft, ArrowRight, Connection, InfoFilled } from '@element-plus/icons-vue';
import { NodeType, EdgeType } from '@/types/api';
import { NODE_STYLE_MAP, EDGE_STYLE_MAP } from '@/types/graph';

// 定义属性
const props = defineProps<{
  selectedItem?: any;
  itemType?: 'node' | 'edge';
  loading?: boolean;
  error?: string;
  graphData?: {
    nodes: any[];
    edges: any[];
  };
}>();

// 定义事件
const emit = defineEmits<{
  (e: 'expandNeighbors', nodeId: string): void;
}>();

// 路由实例
const router = useRouter();

// 面板折叠状态
const isCollapsed = ref(false);

// 面板标题
const title = computed(() => {
  if (props.loading) {
    return '加载中...';
  }
  
  if (props.error) {
    return '加载失败';
  }
  
  if (!props.selectedItem) {
    return '详细信息';
  }
  
  if (props.itemType === 'node') {
    return '节点详情';
  }
  
  if (props.itemType === 'edge') {
    return '关系详情';
  }
  
  return '详细信息';
});

// 切换面板折叠状态
const toggleCollapse = () => {
  isCollapsed.value = !isCollapsed.value;
};

// 获取节点颜色
const getNodeColor = (type: NodeType) => {
  return NODE_STYLE_MAP[type]?.color || '#5B8FF9';
};

// 获取节点图标
const getNodeIcon = (type: NodeType) => {
  switch (type) {
    case NodeType.DATABASE:
      return '🗄️';
    case NodeType.SCHEMA:
      return '📁';
    case NodeType.TABLE:
      return '📋';
    case NodeType.VIEW:
      return '👁️';
    case NodeType.COLUMN:
      return '📊';
    case NodeType.SQL_PATTERN:
      return '⚙️';
    case NodeType.FUNCTION:
      return '🔧';
    default:
      return '📄';
  }
};

// 获取节点类型标签
const getNodeTypeLabel = (type: NodeType) => {
  switch (type) {
    case NodeType.DATABASE:
      return '数据库';
    case NodeType.SCHEMA:
      return '模式';
    case NodeType.TABLE:
      return '表';
    case NodeType.VIEW:
      return '视图';
    case NodeType.COLUMN:
      return '列';
    case NodeType.SQL_PATTERN:
      return 'SQL模式';
    case NodeType.FUNCTION:
      return '函数';
    default:
      return '未知类型';
  }
};

// 获取边颜色
const getEdgeColor = (type: EdgeType) => {
  return EDGE_STYLE_MAP[type]?.color || '#aaa';
};

// 获取边类型标签
const getEdgeTypeLabel = (type: EdgeType) => {
  switch (type) {
    case EdgeType.CONTAINS:
      return '包含';
    case EdgeType.REFERENCES:
      return '引用';
    case EdgeType.DEPENDS_ON:
      return '依赖';
    case EdgeType.DATA_FLOW:
      return '数据流';
    case EdgeType.GENERATES_FLOW:
      return '生成流';
    case EdgeType.WRITES:
      return '写入';
    case EdgeType.READS:
      return '读取';
    default:
      return '未知关系';
  }
};

// 获取源节点标签
const getSourceNodeLabel = () => {
  if (!props.selectedItem || !props.graphData) return '';
  
  const sourceNode = props.graphData.nodes.find(node => node.id === props.selectedItem.source);
  return sourceNode ? sourceNode.label : props.selectedItem.source;
};

// 获取目标节点标签
const getTargetNodeLabel = () => {
  if (!props.selectedItem || !props.graphData) return '';
  
  const targetNode = props.graphData.nodes.find(node => node.id === props.selectedItem.target);
  return targetNode ? targetNode.label : props.selectedItem.target;
};

// 格式化属性键
const formatPropertyKey = (key: string | number) => {
  return String(key)
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, str => str.toUpperCase());
};

// 格式化属性值
const formatPropertyValue = (value: any) => {
  if (value === null || value === undefined) {
    return '-';
  }
  
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  
  return String(value);
};

// 查看详情
const viewDetails = () => {
  if (!props.selectedItem || props.itemType !== 'node') return;
  
  router.push({
    name: 'object-details',
    params: {
      type: props.selectedItem.type,
      fqn: encodeURIComponent(props.selectedItem.fqn || props.selectedItem.id)
    }
  });
};

// 展开邻居
const expandNeighbors = () => {
  if (!props.selectedItem || props.itemType !== 'node') return;
  
  emit('expandNeighbors', props.selectedItem.id);
};
</script>

<style lang="scss" scoped>
.info-panel {
  position: relative;
  width: 300px;
  height: 100%;
  background-color: #fff;
  border-left: 1px solid var(--border-color);
  transition: width 0.3s ease;
  overflow: hidden;
  
  &.is-collapsed {
    width: 40px;
    
    .panel-header {
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      height: 100%;
      padding: 10px 5px;
      
      h3 {
        margin-bottom: 20px;
      }
    }
  }
  
  .panel-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 15px;
    border-bottom: 1px solid var(--border-color);
    background-color: #f5f7fa;
    
    h3 {
      margin: 0;
      font-size: 16px;
      font-weight: 600;
      color: var(--text-color);
    }
  }
  
  .panel-content {
    height: calc(100% - 41px);
    overflow-y: auto;
    padding: 15px;
    
    .panel-loading,
    .panel-error,
    .empty-state {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      height: 200px;
      color: #909399;
      
      p {
        margin-top: 10px;
      }
      
      .el-icon {
        font-size: 48px;
        margin-bottom: 10px;
      }
    }
    
    .item-header {
      display: flex;
      align-items: center;
      margin-bottom: 15px;
      
      .item-icon {
        width: 40px;
        height: 40px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        margin-right: 10px;
        color: #fff;
        font-size: 18px;
      }
      
      .item-title {
        h4 {
          margin: 0 0 5px;
          font-size: 16px;
          font-weight: 600;
          color: var(--text-color);
        }
        
        .item-type {
          font-size: 12px;
          color: #909399;
        }
      }
    }
    
    .section-title {
      margin: 20px 0 10px;
      font-size: 14px;
      font-weight: 600;
      color: var(--text-color);
    }
    
    .actions-container {
      margin-top: 20px;
      display: flex;
      gap: 10px;
    }
  }
}
</style>
