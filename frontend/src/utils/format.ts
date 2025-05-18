/**
 * 格式化工具函数
 * 
 * 作者: Vance Chen
 */

/**
 * 格式化日期时间
 * 
 * @param dateString 日期字符串
 * @param includeTime 是否包含时间
 * @returns 格式化后的日期时间字符串
 */
export function formatDate(dateString: string | null | undefined, includeTime = true): string {
  if (!dateString) return '未知';
  
  try {
    const date = new Date(dateString);
    
    // 检查日期是否有效
    if (isNaN(date.getTime())) {
      return '无效日期';
    }
    
    // 格式化日期部分
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    
    const dateFormatted = `${year}-${month}-${day}`;
    
    // 如果不需要时间部分，直接返回日期
    if (!includeTime) {
      return dateFormatted;
    }
    
    // 格式化时间部分
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    
    return `${dateFormatted} ${hours}:${minutes}:${seconds}`;
  } catch (error) {
    console.error('日期格式化错误:', error);
    return '格式化错误';
  }
}

/**
 * 格式化文件大小
 * 
 * @param bytes 字节数
 * @param decimals 小数位数
 * @returns 格式化后的文件大小字符串
 */
export function formatFileSize(bytes: number, decimals = 2): string {
  if (bytes === 0) return '0 字节';
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['字节', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * 截断文本
 * 
 * @param text 原始文本
 * @param maxLength 最大长度
 * @returns 截断后的文本
 */
export function truncateText(text: string, maxLength: number): string {
  if (!text) return '';
  
  if (text.length <= maxLength) {
    return text;
  }
  
  return text.substring(0, maxLength) + '...';
}
