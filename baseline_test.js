import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { htmlReport } from 'https://raw.githubusercontent.com/benc-uk/k6-reporter/main/dist/bundle.js';

const errorRate = new Rate('errors');
const responseTime = new Trend('response_time');

export const options = {
  stages: [
    { duration: '10s', target: 200 }, 
    { duration: '30s', target: 200 }, 
    { duration: '10s', target: 0 },  
  ],
  thresholds: {
    'http_req_duration': ['p(95)<500'], 
    'errors': ['rate<0.01'],            
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const concertId = Math.floor(Math.random() * 1000) + 1;
  const memberId = Math.floor(Math.random() * 10000) + 1;

  const payload = JSON.stringify({
    concertId: concertId,
    memberId: memberId,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const response = http.post(`${BASE_URL}/api/reservations`, payload, params);
  
  responseTime.add(response.timings.duration);

  const success = check(response, {
    'status is 201': (r) => r.status === 201,
    'response has body': (r) => r.body.length > 0,
  });

  errorRate.add(!success);
}

export function handleSummary(data) {
  return {
    'stdout': textSummary(data, { indent: ' ', enableColors: true }),
    'summary.json': JSON.stringify(data, null, 2),
  };
}

// 텍스트 요약 함수
function textSummary(data, options = {}) {
  const indent = options.indent || '';
  const enableColors = options.enableColors || false;
  
  let summary = '\n';
  summary += `${indent}========== K6 부하 테스트 결과 요약 ==========\n`;
  summary += `${indent}테스트 시나리오: Ramp-up(10s) -> Steady State(30s) -> Ramp-down(10s)\n`;
  summary += `${indent}최대 가상 유저 수: 200명\n`;
  summary += `${indent}총 테스트 시간: 50초\n\n`;
  
  if (data.metrics.http_req_duration) {
    const duration = data.metrics.http_req_duration;
    summary += `${indent}[응답 시간]\n`;
    summary += `${indent}  평균: ${duration.values.avg.toFixed(2)} ms\n`;
    summary += `${indent}  P95: ${duration.values['p(95)'].toFixed(2)} ms\n`;
    summary += `${indent}  P99: ${duration.values['p(99)'].toFixed(2)} ms\n`;
    summary += `${indent}  최소: ${duration.values.min.toFixed(2)} ms\n`;
    summary += `${indent}  최대: ${duration.values.max.toFixed(2)} ms\n\n`;
  }
  
  if (data.metrics.http_reqs) {
    const reqs = data.metrics.http_reqs;
    summary += `${indent}[요청 통계]\n`;
    summary += `${indent}  총 요청 수: ${reqs.values.count}\n`;
    summary += `${indent}  초당 요청 수: ${reqs.values.rate.toFixed(2)} req/s\n\n`;
  }
  
  if (data.metrics.http_req_failed) {
    const failed = data.metrics.http_req_failed;
    summary += `${indent}[에러 통계]\n`;
    summary += `${indent}  총 에러 수: ${failed.values.count}\n`;
    summary += `${indent}  에러율: ${(failed.values.rate * 100).toFixed(2)}%\n\n`;
  }
  
  if (data.root_group.checks) {
    summary += `${indent}[Thresholds 검증]\n`;
    data.root_group.checks.forEach(check => {
      const status = check.passes ? '✓ 통과' : '✗ 실패';
      summary += `${indent}  ${check.name}: ${status}\n`;
    });
    summary += '\n';
  }
  
  summary += `${indent}==========================================\n`;
  
  return summary;
}

