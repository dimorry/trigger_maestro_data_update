import unittest
from unittest.mock import patch, MagicMock
from oauth import get_headers, get_access_token
from trigger_data_update import make_request, trigger_data_update, check_data_update_status

class TestOAuth(unittest.TestCase):
    @patch("oauth.base64.b64encode")
    def test_get_headers_with_access_token(self, mock_b64encode):
        headers = get_headers("test_token")
        self.assertEqual(headers["Authorization"], "Bearer test_token")
        self.assertEqual(headers["Content-Type"], "application/json")

    @patch("oauth.requests.post")
    def test_get_access_token(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_token"}
        mock_post.return_value = mock_response

        token = get_access_token("https://example.com")
        self.assertEqual(token, "test_token")
        mock_post.assert_called_once()

class TestTriggerDataUpdate(unittest.TestCase):
    @patch("trigger_data_update.requests.post")
    def test_make_request_post(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        response = make_request("POST", "https://example.com", {"key": "value"}, {"data": "test"})
        self.assertEqual(response.status_code, 200)
        mock_post.assert_called_once()

    @patch("trigger_data_update.requests.get")
    def test_make_request_get(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        response = make_request("GET", "https://example.com", {"key": "value"})
        self.assertEqual(response.status_code, 200)
        mock_get.assert_called_once()

    @patch("trigger_data_update.make_request")
    def test_trigger_data_update(self, mock_make_request):
        mock_make_request.return_value = MagicMock(json=lambda: {"StatusKey": "test_status_key"})

        status_key = trigger_data_update("https://example.com", "test_data_source", "test_token")
        self.assertEqual(status_key, "test_status_key")
        mock_make_request.assert_called_once()

    @patch("trigger_data_update.make_request")
    def test_check_data_update_status(self, mock_make_request):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_make_request.return_value = mock_response

        response = check_data_update_status("https://example.com", "test_status_key", "test_token")
        self.assertEqual(response.status_code, 200)
        mock_make_request.assert_called_once()

if __name__ == "__main__":
    unittest.main()
