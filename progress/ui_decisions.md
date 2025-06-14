# 血缘关系可视化 UI 设计文档

**最后更新时间**：2025-05-26

## 技术栈选择

- **前端框架**：Vue 3 + TypeScript
- **UI 组件库**：Element Plus
- **图形可视化**：AntV G6
- **构建工具**：Vite

## 核心功能设计

### 1. 血缘图布局设计

#### 1.1 节点层级划分
- **目标端（左侧）**：
  - 数据库节点 (layer=0)
  - Schema节点 (layer=1)
  - 目标表节点 (layer=2)
  - 目标字段节点 (layer=3)
- **源端（右侧）**：
  - 源表/字段节点 (layer=5，同级展示)
  - 其他辅助节点 (layer=2)

#### 1.2 边类型处理
- **血缘关系 (data_flow)**：
  - 使用自定义边类型 `custom-data-flow`
  - 箭头方向：从目标指向源（视觉上从右到左）
  - 使用二次贝塞尔曲线实现优雅的弯曲效果
  - 源端节点（表和字段）同级展示，避免不必要的层级嵌套
- **元数据关系**：
  - `has_schema`、`has_object`、`has_column` 等使用折线连接
  - 保持从左到右的层次结构展示

### 2. 交互设计

#### 2.1 基础操作
- 缩放：支持鼠标滚轮和工具栏按钮
- 平移：支持拖拽画布
- 适应画布：自动调整视图以显示所有节点
- 小地图：提供全局视图导航

#### 2.2 节点交互
- 点击：选中节点，显示详细信息
- 悬停：高亮显示节点及其关联边
- 拖拽：支持节点位置微调

#### 2.3 边交互
- 点击：选中边，显示血缘关系详情
- 悬停：高亮显示边及其关联节点

### 3. 视觉设计

#### 3.1 节点样式
- 数据库：圆形图标 + 数据库名称
- 表：矩形图标 + 表名
- 字段：圆形节点 + 字段名
- 其他类型：根据节点类型使用不同形状和颜色

#### 3.2 边样式
- 血缘关系：红色粗线 + 箭头
- 元数据关系：灰色虚线
- 写入关系：绿色实线

### 4. 性能优化

#### 4.1 渲染优化
- 使用 G6 的节点和边缓存机制
- 按需渲染，避免一次性加载过多数据
- 优化节点和边的样式计算

#### 4.2 布局优化
- 使用 dagre 布局算法
- 优化节点间距和层级间距
- 支持布局切换（层次布局、力导向布局等）

## 待优化项目

1. **血缘关系展示**：
   - [x] 优化血缘关系的曲线展示
   - [x] 调整源端节点层级，实现同级展示
   - [ ] 优化大量血缘关系时的边交叉问题

2. **交互体验**：
   - [ ] 添加节点搜索和过滤功能
   - [ ] 实现血缘路径高亮
   - [ ] 添加节点展开/收起功能

3. **性能优化**：
   - [ ] 实现大规模图谱的分批加载
   - [ ] 优化节点和边的渲染性能
   - [ ] 添加图谱数据的本地缓存

4. **其他功能**：
   - [ ] 支持图谱导出（PNG、SVG、JSON）
   - [ ] 添加图谱编辑功能
   - [ ] 实现血缘关系的时序展示

## 设计决策记录

### 2025-05-26
1. **血缘关系方向调整**
   - 问题：血缘关系的箭头方向与数据流向不一致
   - 决策：在前端渲染时反转 source/target，保持数据流向正确性
   - 实现：修改 `processGraphData` 函数，对 data_flow 类型的边进行特殊处理

2. **源端节点层级优化**
   - 问题：源端表和字段分属不同层级，导致血缘关系展示不直观
   - 决策：将源端表和字段统一到同一层级（layer=5）
   - 实现：修改 `calculateNodeLayer` 函数，调整源端节点的层级计算逻辑

3. **自定义血缘关系边**
   - 问题：G6 内置边类型无法满足血缘关系的展示需求
   - 决策：实现自定义边类型 `custom-data-flow`
   - 实现：使用二次贝塞尔曲线，优化控制点计算，实现优雅的弯曲效果
