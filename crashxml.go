package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// CrashContext ---------- XML 모델 ----------
type CrashContext struct {
	XMLName  xml.Name `xml:"FGenericCrashContext"`
	GameData GameData `xml:"GameData"`
}
type GameData struct {
	RipperBuildNumber string `xml:"RipperBuildNumber"`
	RipperBuildDate   string `xml:"RipperBuildDate"`
	RipperVersion     string `xml:"RipperVersion"`
	LibUnrealBuildID  string `xml:"LibUnrealBuildID"`
}

// ---------- Telegram 응답 최소 모델 ----------
type tgResponse struct {
	OK          bool   `json:"ok"`
	Description string `json:"description"`
}

// ---------- XML 읽기 ----------
func ReadCrashXML(path string) (*CrashContext, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	var ctx CrashContext
	dec := xml.NewDecoder(f)
	if err := dec.Decode(&ctx); err != nil && err != io.EOF {
		return nil, fmt.Errorf("xml decode: %w", err)
	}
	return &ctx, nil
}

// ---------- Telegram 메시지 전송 ----------
func SendTelegramMessage(token, chatID, text string) error {
	if token == "" || chatID == "" {
		return fmt.Errorf("token/chatID is empty")
	}
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)

	form := url.Values{}
	form.Set("chat_id", chatID)
	form.Set("text", text)         // parse_mode 사용 안 함(이스케이프 불필요)
	form.Set("parse_mode", "HTML") // HTML 모드

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.PostForm(endpoint, form)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	// Telegram은 HTTP 200이어도 ok=false일 수 있으니 JSON 검사
	var tr tgResponse
	_ = json.NewDecoder(resp.Body).Decode(&tr)
	if resp.StatusCode != http.StatusOK || !tr.OK {
		return fmt.Errorf("telegram error: status=%s ok=%v desc=%q",
			resp.Status, tr.OK, tr.Description)
	}
	return nil
}
