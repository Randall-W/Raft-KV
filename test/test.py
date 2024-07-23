from locust import User, task, between, events
import redis
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisUser(User):
    wait_time = between(1, 5)

    def on_start(self):
        self.client = redis.StrictRedis(host='127.0.0.1', port=8084, db=0)
        logger.info("Redis connection successful")

    @task
    def set_key(self):
        logger.info("Executing set_key task")
        start_time = time.time()
        try:
            response = self.client.set('test_key', 'test_value')
            logger.info(f"SET response: {response}")
            if response is True:
                total_time = int((time.time() - start_time) * 1000)
                events.request.fire(request_type="SET", name="test_key", response_time=total_time, response_length=0, exception=None)
                logger.info("SET request event fired")
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request.fire(request_type="SET", name="test_key", response_time=total_time, response_length=0, exception=Exception("SET operation failed"))
                logger.info("SET request event fired with failure")
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(request_type="SET", name="test_key", response_time=total_time, response_length=0, exception=e)
            logger.error(f"SET operation failed: {e}")

    @task
    def get_key(self):
        logger.info("Executing get_key task")
        start_time = time.time()
        try:
            response = self.client.get('test_key')
            logger.info(f"GET response: {response}")
            if response is not None:
                total_time = int((time.time() - start_time) * 1000)
                events.request.fire(request_type="GET", name="test_key", response_time=total_time, response_length=len(response), exception=None)
                logger.info("GET request event fired")
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request.fire(request_type="GET", name="test_key", response_time=total_time, response_length=0, exception=Exception("GET operation failed"))
                logger.info("GET request event fired with failure")
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request.fire(request_type="GET", name="test_key", response_time=total_time, response_length=0, exception=e)
            logger.error(f"GET operation failed: {e}")
