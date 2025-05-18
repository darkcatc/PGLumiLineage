/**
 * 图可视化类型定义
 * 
 * 此文件定义了与 AntV G6 图可视化相关的数据类型
 * 
 * 作者: Vance Chen
 */

import { NodeType, EdgeType } from './api';

// G6 节点样式配置
export interface NodeStyle {
  shape: string;
  size: number | number[];
  color: string;
  icon?: {
    type: string;
    fill?: string;
    fontFamily?: string;
  };
  labelCfg?: {
    position?: string;
    offset?: number;
    style?: {
      fill?: string;
      fontSize?: number;
    };
  };
  style?: {
    fill?: string;
    stroke?: string;
    lineWidth?: number;
    lineDash?: number[];
    shadowColor?: string;
    shadowBlur?: number;
    radius?: number;
  };
  stateStyles?: {
    hover?: Record<string, any>;
    selected?: Record<string, any>;
  };
}

// G6 边样式配置
export interface EdgeStyle {
  type: string;
  color: string;
  size: number;
  style?: {
    stroke?: string;
    lineWidth?: number;
    lineDash?: number[];
    opacity?: number;
    endArrow?: boolean | Record<string, any>;
    startArrow?: boolean | Record<string, any>;
  };
  labelCfg?: {
    position?: string;
    autoRotate?: boolean;
    style?: {
      fill?: string;
      fontSize?: number;
    };
  };
  stateStyles?: {
    hover?: Record<string, any>;
    selected?: Record<string, any>;
  };
}

// 节点类型样式映射
export const NODE_STYLE_MAP: Record<NodeType, NodeStyle> = {
  [NodeType.DATABASE]: {
    shape: 'circle',
    size: 50,
    color: '#3498db',
    icon: {
      type: 'database',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#3498db',
      stroke: '#2980b9',
      lineWidth: 2,
      shadowColor: '#3498db',
      shadowBlur: 10
    },
    stateStyles: {
      hover: {
        lineWidth: 3,
        shadowBlur: 15
      },
      selected: {
        lineWidth: 4,
        shadowBlur: 20,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.SCHEMA]: {
    shape: 'hexagon',
    size: 40,
    color: '#2ecc71',
    icon: {
      type: 'folder',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#2ecc71',
      stroke: '#27ae60',
      lineWidth: 2
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.TABLE]: {
    shape: 'rect',
    size: [35, 35],
    color: '#e74c3c',
    icon: {
      type: 'table',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#e74c3c',
      stroke: '#c0392b',
      lineWidth: 2,
      radius: 0
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.VIEW]: {
    shape: 'rect',
    size: [35, 35],
    color: '#9b59b6',
    icon: {
      type: 'eye',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#9b59b6',
      stroke: '#8e44ad',
      lineWidth: 2,
      radius: 5
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.COLUMN]: {
    shape: 'ellipse',
    size: [25, 20],
    color: '#f1c40f',
    icon: {
      type: 'column',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#f1c40f',
      stroke: '#f39c12',
      lineWidth: 2
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.SQL_PATTERN]: {
    shape: 'diamond',
    size: 35,
    color: '#e67e22',
    icon: {
      type: 'gear',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#e67e22',
      stroke: '#d35400',
      lineWidth: 2
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [NodeType.FUNCTION]: {
    shape: 'triangle',
    size: 35,
    color: '#1abc9c',
    icon: {
      type: 'function',
      fill: '#fff',
      fontFamily: 'iconfont'
    },
    style: {
      fill: '#1abc9c',
      stroke: '#16a085',
      lineWidth: 2
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#3498db'
      }
    }
  }
};

// 边类型样式映射
export const EDGE_STYLE_MAP: Record<EdgeType, EdgeStyle> = {
  [EdgeType.CONTAINS]: {
    type: 'line',
    color: '#7f8c8d',
    size: 1,
    style: {
      stroke: '#7f8c8d',
      lineWidth: 1,
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#7f8c8d'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 2
      },
      selected: {
        lineWidth: 3,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.REFERENCES]: {
    type: 'line',
    color: '#3498db',
    size: 1,
    style: {
      stroke: '#3498db',
      lineWidth: 1,
      lineDash: [5, 5],
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#3498db'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 2
      },
      selected: {
        lineWidth: 3,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.DEPENDS_ON]: {
    type: 'line',
    color: '#95a5a6',
    size: 1,
    style: {
      stroke: '#95a5a6',
      lineWidth: 1,
      lineDash: [2, 2],
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#95a5a6'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 2
      },
      selected: {
        lineWidth: 3,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.DATA_FLOW]: {
    type: 'cubic',
    color: '#e74c3c',
    size: 2,
    style: {
      stroke: '#e74c3c',
      lineWidth: 2,
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#e74c3c'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.GENERATES_FLOW]: {
    type: 'cubic',
    color: '#e67e22',
    size: 2,
    style: {
      stroke: '#e67e22',
      lineWidth: 2,
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#e67e22'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.WRITES]: {
    type: 'cubic',
    color: '#2ecc71',
    size: 2,
    style: {
      stroke: '#2ecc71',
      lineWidth: 2,
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#2ecc71'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  },
  [EdgeType.READS]: {
    type: 'cubic',
    color: '#9b59b6',
    size: 2,
    style: {
      stroke: '#9b59b6',
      lineWidth: 2,
      endArrow: {
        path: 'M 0,0 L 8,4 L 0,8 Z',
        fill: '#9b59b6'
      }
    },
    stateStyles: {
      hover: {
        lineWidth: 3
      },
      selected: {
        lineWidth: 4,
        stroke: '#1abc9c'
      }
    }
  }
};
