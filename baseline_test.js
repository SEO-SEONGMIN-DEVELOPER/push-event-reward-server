import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

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

export function setup() {
  console.log('데이터베이스 초기화를 시작합니다...');
  
  const initResponse = http.post(`${BASE_URL}/api/test/init`, null, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  });

  if (initResponse.status === 201) {
    const body = JSON.parse(initResponse.body);
    console.log(`✓ 데이터베이스 초기화 완료: Concert ${body.concertCount}개, Member ${body.memberCount}개`);
    console.log(`  Concert IDs: ${body.concertIds.join(', ')}`);
    console.log(`  Member ID 범위: ${body.memberIds[0]} ~ ${body.memberIds[body.memberIds.length - 1]}`);
    
    return { 
      initialized: true,
      concertIds: body.concertIds,
      memberIds: body.memberIds
    };
  } else {
    console.error(`✗ 데이터베이스 초기화 실패: Status ${initResponse.status}, Body: ${initResponse.body}`);
    throw new Error('데이터베이스 초기화에 실패했습니다. 테스트를 중단합니다.');
  }
}

export default function (data) {
  const concertIds = data.concertIds || [1, 2, 3, 4, 5];
  const memberIds = data.memberIds || Array.from({length: 200}, (_, i) => i + 1);
  
  // const concertId = concertIds[Math.floor(Math.random() * concertIds.length)];
  const concertId = concertIds[0]
  const memberId = memberIds[Math.floor(Math.random() * memberIds.length)];

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

  if (response.status !== 201) {
    let errorType = 'Unknown Error';
    let errorMessage = '';
    
    if (response.status === 0) {
      errorType = 'Connection Failed - 애플리케이션이 실행 중이지 않거나 네트워크 오류';
    } else if (response.status === 500) {
      errorType = 'Server Error (500)';
    } else if (response.status === 503) {
      errorType = 'Service Unavailable (503) - 쓰레드 풀이 부족합니다.';
    } else if (response.status === 404) {
      errorType = 'Not Found (404)';
    } else if (response.status === 400) {
      errorType = 'Bad Request (400)';  
    } else if (response.status === 409) {
      errorType = 'Conflict (409)';
    }
    
    try {
      const body = JSON.parse(response.body);
      if (body.message) {
        errorMessage = body.message;
      } else if (body.error) {
        errorMessage = body.error;
      }
    } catch (e) {
      errorMessage = response.body.substring(0, 200);
    }
    
    console.log(`[ERROR] Type: ${errorType}, Status: ${response.status}, Message: ${errorMessage}, Request: concertId=${concertId}, memberId=${memberId}`);
  }

  const success = check(response, {
    'status is 201': (r) => r.status === 201,
    'response has body': (r) => r.body.length > 0,
  });

  errorRate.add(!success);
}

