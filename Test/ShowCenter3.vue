<template>
  <div class="show-center">
    <div class="grid-container">
      <!-- 左侧信息区域 -->
      <div class="grid-item info-panel">
        <!-- 储能电站信息 -->
        <div class="chart-box">
          <div class="chart-title">储能电站信息</div>
          <div class="info-table">
            <div class="info-row">
              <span class="label">储能电站配置:</span>
              <span class="value">20 / 172</span>
              <span class="unit">MW/kWh</span>
            </div>
            <div class="info-row">
              <span class="label">飞轮储能系统配置:</span>
              <span class="value">20 / 172</span>
              <span class="unit">MW/kWh</span>
            </div>
            <!-- 其他信息行 -->
          </div>
        </div>
        
        <!-- 储能电站运行统计数据 -->
        <div class="chart-box">
          <div class="chart-title">储能电站运行统计数据</div>
          <div class="info-table">
            <!-- 运行统计数据内容 -->
          </div>
        </div>
        
        <!-- 储能电站实时数据 -->
        <div class="chart-box">
          <div class="chart-title">储能电站实时数据</div>
          <div class="info-table">
            <!-- 实时数据内容 -->
          </div>
        </div>
      </div>

      <!-- 中间区域 -->
      <div class="grid-item center-panel">
        <!-- 站点标题 -->
        <div class="station-title">微控厂内测试区</div>
        
        <!-- 站点图片 -->
        <div class="station-image">
          <!-- <img src="../assets/station.jpg" alt="储能电站"> -->
        </div>
        
        <!-- 频率对比分析图表 -->
        <div class="chart-box">
          <div class="chart-title">频率对比分析</div>
          <div ref="frequencyChart" class="chart"></div>
        </div>
      </div>

      <!-- 右侧统计图表 -->
      <div class="grid-item stats-panel">
        <!-- 一次调频里程分布 -->
        <div class="chart-box">
          <div class="chart-title">一次调频里程分布</div>
          <div ref="distributionChart1" class="pie-chart"></div>
        </div>
        
        <!-- 单次动作时长分布 -->
        <div class="chart-box">
          <div class="chart-title">单次动作时长分布</div>
          <div ref="distributionChart2" class="pie-chart"></div>
        </div>
        
        <!-- 电站SOC分布 -->
        <div class="chart-box">
          <div class="chart-title">电站SOC分布</div>
          <div ref="distributionChart3" class="pie-chart"></div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue';
import * as echarts from 'echarts';

const frequencyChart = ref(null);
const distributionChart1 = ref(null);
const distributionChart2 = ref(null);
const distributionChart3 = ref(null);
const charts = [];
let socket = null; // WebSocket连接

// 用来保存通过 WebSocket 接收到的数据
const wsData = ref({
  soc: [15.0, 0.0, 5.0, 5.0, 35.0, 10.0, 10.0, 5.0, 15.0],
  frequency: [0.0, 10.0, 10.0, 5.0, 35.0, 0.0, 15.0, 10.0, 15.0],
  duration: [0.0, 5.0, 5.0, 20.0, 40.0, 10.0, 5.0, 15.0, 0.0]
});

// 连接 WebSocket
const connectWebSocket = () => {
  socket = new WebSocket('ws://192.168.102.75:10000/ws/ShowCenter/'); // 替换为实际的 WebSocket URL

  socket.onopen = () => {
    console.log('WebSocket connected');
  };

  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    wsData.value = message.message; // 更新数据
    updateCharts(); // 更新图表
  };

  socket.onerror = (error) => {
    console.error('WebSocket error:', error);
  };

  socket.onclose = () => {
    console.log('WebSocket disconnected');
  };
};

// 关闭 WebSocket 连接
const closeWebSocket = () => {
  if (socket) {
    socket.close();
  }
};

// 更新图表数据
const updateCharts = () => {
  // 更新频率对比图
  const freqChart = echarts.init(frequencyChart.value);
  freqChart.setOption({
    grid: {
      top: 30,
      bottom: 30,
      left: 60,
      right: 40
    },
    tooltip: {
      trigger: 'axis',
      formatter: function(params) {
        const date = new Date(params[0].value[0]);
        return `${date.toLocaleTimeString()}<br/>
                频率: ${params[0].value[1].toFixed(3)} Hz`;
      }
    },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: '#1890ff' } },
      axisLabel: { color: '#fff' },
      splitLine: { show: false }
    },
    yAxis: {
      type: 'value',
      name: '频率(Hz)',
      nameTextStyle: { color: '#fff' },
      min: -2,
      max: 2,
      interval: 1,
      axisLine: { lineStyle: { color: '#1890ff' } },
      axisLabel: { color: '#fff' },
      splitLine: {
        show: true,
        lineStyle: { color: 'rgba(24, 144, 255, 0.1)' }
      }
    },
    series: [{
      name: '频率',
      type: 'line',
      data: generateTimeData(),
      lineStyle: { color: '#00ff00', width: 1 },
      symbol: 'none',
      animation: false
    }]
  });
  charts.push(freqChart);

  // 更新饼图数据
  const pieCharts = [
    { ref: distributionChart1, type: 'frequency', title: '一次调频里程分布' },
    { ref: distributionChart2, type: 'duration', title: '单次动作时长分布' },
    { ref: distributionChart3, type: 'soc', title: '电站SOC分布' }
  ];

  pieCharts.forEach(({ ref, type, title }) => {
    const chart = echarts.init(ref.value);
    chart.setOption(pieOption(type, title)); // 更新饼图数据
  });
};

// 生成饼图数据
const generatePieData = (type) => {
  switch (type) {
    case 'frequency': // 一次调频里程分布
      return wsData.value.frequency.map((value, index) => ({
        value,
        name: `${index * 10}-${(index + 1) * 10}%`,
        itemStyle: { color: ['#1890ff', '#36cfc9', '#73d13d', '#ffc53d', '#ff4d4f'][index] }
      }));
    case 'duration': // 单次动作时长分布
      return wsData.value.duration.map((value, index) => ({
        value,
        name: `${index * 5}-${(index + 1) * 5}s`,
        itemStyle: { color: ['#1890ff', '#36cfc9', '#73d13d', '#ffc53d', '#ff4d4f'][index] }
      }));
    case 'soc': // 电站SOC分布
      return wsData.value.soc.map((value, index) => ({
        value,
        name: `${index * 10}-${(index + 1) * 10}%`,
        itemStyle: { color: ['#1890ff', '#36cfc9', '#73d13d', '#ffc53d', '#ff4d4f'][index] }
      }));
  }
};

// 饼图通用配置
const pieOption = (type, title) => ({
  tooltip: {
    trigger: 'item',
    formatter: '{b}: {c}%'
  },
  legend: {
    orient: 'vertical',
    right: 10,
    top: 'center',
    textStyle: { color: '#fff' },
    formatter: name => {
      const data = generatePieData(type);
      const item = data.find(item => item.name === name);
      return `${name} ${item.value}%`;
    }
  },
  series: [{
    name: title,
    type: 'pie',
    radius: '60%',
    center: ['50%', '50%'],
    data: generatePieData(type),
    itemStyle: { borderWidth: 2, borderColor: '#fff' }
  }]
});

// 初始化图表
const initCharts = () => {
  // 初始化频率对比分析图表
  const freqChart = echarts.init(frequencyChart.value);
  freqChart.setOption({ /* 配置项 */ });

  // 初始化三个饼图
  const pieCharts = [
    { ref: distributionChart1, type: 'frequency', title: '一次调频里程分布' },
    { ref: distributionChart2, type: 'duration', title: '单次动作时长分布' },
    { ref: distributionChart3, type: 'soc', title: '电站SOC分布' }
  ];

  pieCharts.forEach(({ ref, type, title }) => {
    const chart = echarts.init(ref.value);
    chart.setOption(pieOption(type, title)); // 初始化饼图
  });

  charts.push(freqChart);
  pieCharts.forEach(({ ref }) => {
    const chart = echarts.init(ref.value);
    charts.push(chart);
  });
};

// 生成时间数据
const generateTimeData = () => {
  return wsData.value.frequency.map((value, index) => [
    new Date().getTime() + index * 1000, // 时间戳
    value // 数据值
  ]);
};

// 在组件加载时连接 WebSocket
onMounted(() => {
  connectWebSocket(); // 初始化 WebSocket 连接
  initCharts(); // 初始化图表
});

// 在组件卸载时关闭 WebSocket 连接
onUnmounted(() => {
  closeWebSocket(); // 关闭 WebSocket 连接
});
</script>

<style lang="scss" scoped>
.show-center {
  width: 100%;
  height: 100%;
  color: white;
  
  .grid-container {
    display: grid;
    grid-template-columns: 25% 50% 25%;
    gap: 10px;
    height: 100%;
    padding: 10px;
    
    .grid-item {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
  }
}

.chart-box {
  background: rgba(0, 21, 41, 0.7);
  border: 1px solid #1890ff;
  border-radius: 4px;
  padding: 10px;
  height: 300px;
  
  .chart-title {
    position: relative;
    color: transparent;
    background: linear-gradient(120deg, #1890ff 20%, #00c6fb 40%, #1890ff 60%, #00c6fb 80%);
    background-size: 200% auto;
    background-clip: text;
    -webkit-background-clip: text;
    font-size: 18px;
    font-weight: bold;
    padding: 8px 0;
    margin-bottom: 5px;
    border-bottom: 1px solid rgba(24, 144, 255, 0.3);
    animation: shine 3s linear infinite;
    text-shadow: 0 0 10px rgba(24, 144, 255, 0.3);
    
    &::after {
      content: '';
      position: absolute;
      bottom: -1px;
      left: 0;
      width: 100%;
      height: 2px;
      background: linear-gradient(90deg, 
        transparent,
        #00c6fb,
        #1890ff,
        #00c6fb,
        transparent
      );
      animation: flow 3s linear infinite;
    }
  }
}

@keyframes shine {
  0% { background-position: 200% center; }
  100% { background-position: -200% center; }
}

@keyframes flow {
  0% { background-position: -200% center; }
  100% { background-position: 200% center; }
}

.info-table {
  .info-row {
    display: flex;
    padding: 8px 0;
    
    .label {
      color: #8b8b8b;
      flex: 2;
    }
    
    .value {
      color: #00c6fb;
      flex: 1;
      text-align: right;
    }
    
    .unit {
      color: #8b8b8b;
      margin-left: 5px;
      width: 60px;
    }
  }
}

.station-title {
  text-align: center;
  font-size: 24px;
  color: #00c6fb;
  padding: 10px 0;
}

.station-image {
  flex: 1;
  margin: 10px 0;
  
  img {
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 4px;
  }
}

.chart {
  height: 200px;
}

.pie-chart {
  height: 246px;
}
</style>