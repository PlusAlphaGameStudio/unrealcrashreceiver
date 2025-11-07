package main

import (
	"bufio"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kardianos/service"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	maxMaxMultipartMemory = 100 << 20
	maxBody               = 100 << 20 // 100 MiB 안전 장치
	maxStringLen          = 1 << 20   // 1 MiB
	maxDataLen            = 100 << 20 // 256 MiB (필요시 조정)
)

type CR1Header struct {
	DirectoryName    string
	FileName         string
	UncompressedSize uint32
	FileCount        uint32
}

type FileEntry struct {
	FileIndex int32
	FileName  string
	Data      []byte
}

// ReadCR1 r에서 CR1 포맷을 읽어들임
func ReadCR1(br io.Reader, order binary.ByteOrder) (*CR1Header, error) {

	// 1) 매직 3바이트 확인
	var magic [3]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("read magic: %w", err)
	}
	if magic != [3]byte{'C', 'R', '1'} {
		return nil, fmt.Errorf("invalid magic: %q", magic)
	}

	// 2) 길이-프리픽스 문자열 2개
	dir, err := readLenPrefixedString(br, order)
	if err != nil {
		return nil, fmt.Errorf("read DirectoryName: %w", err)
	}
	name, err := readLenPrefixedString(br, order)
	if err != nil {
		return nil, fmt.Errorf("read FileName: %w", err)
	}

	// 3) 나머지 32비트 정수 2개
	var uncompressedSize uint32
	if err := binary.Read(br, order, &uncompressedSize); err != nil {
		return nil, fmt.Errorf("read UncompressedSize: %w", err)
	}
	var fileCount uint32
	if err := binary.Read(br, order, &fileCount); err != nil {
		return nil, fmt.Errorf("read FileCount: %w", err)
	}

	return &CR1Header{
		DirectoryName:    dir,
		FileName:         name,
		UncompressedSize: uncompressedSize,
		FileCount:        fileCount,
	}, nil
}

// ReadFileEntry 단일 엔트리 읽기
func ReadFileEntry(br io.Reader, order binary.ByteOrder) (*FileEntry, error) {
	// 1) FileIndex
	var idx int32
	if err := binary.Read(br, order, &idx); err != nil {
		if errors.Is(err, io.EOF) { // 스트림 종료
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read FileIndex: %w", err)
	}

	// 2) FileName (len + bytes)
	name, err := readLenPrefixedString(br, order)
	if err != nil {
		return nil, fmt.Errorf("read FileName: %w", err)
	}

	// 3) Data (len + bytes)
	data, err := readLenPrefixedBytes(br, order)
	if err != nil {
		return nil, fmt.Errorf("read Data: %w", err)
	}

	return &FileEntry{
		FileIndex: idx,
		FileName:  name,
		Data:      data,
	}, nil
}

func readLenPrefixedBytes(r io.Reader, order binary.ByteOrder) ([]byte, error) {
	var n int32
	if err := binary.Read(r, order, &n); err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errors.New("negative data length")
	}
	if n > maxDataLen {
		return nil, fmt.Errorf("data too large: %d > %d", n, maxDataLen)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func readLenPrefixedString(r io.Reader, order binary.ByteOrder) (string, error) {
	var n int32
	if err := binary.Read(r, order, &n); err != nil {
		return "", err
	}
	if n < 0 {
		return "", errors.New("negative string length")
	}
	if n > maxStringLen {
		return "", fmt.Errorf("string too large: %d > %d", n, maxStringLen)
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	// UTF-8이 보장되지 않는 포맷이라면 이 검사를 빼세요.
	if !utf8.Valid(buf) {
		return "", errors.New("string is not valid UTF-8")
	}

	return strings.TrimRight(string(buf), "\x00"), nil
}

// baseDir 밖으로 나가지 못하게 경로를 정화하고 조합
func safeJoin(baseDir, relPath string) (string, error) {
	if relPath == "" {
		return "", fmt.Errorf("empty relative path")
	}
	clean := filepath.Clean(relPath)
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("absolute paths not allowed: %q", relPath)
	}
	full := filepath.Join(baseDir, clean)

	rel, err := filepath.Rel(baseDir, full)
	if err != nil {
		return "", fmt.Errorf("rel: %w", err)
	}
	// ".."로 시작하면 baseDir 벗어남
	if strings.HasPrefix(rel, "..") || rel == "." && clean == "." {
		return "", fmt.Errorf("path escapes baseDir: %q", relPath)
	}
	return full, nil
}

// WriteFileEntry baseDir 아래에 e.FileName 안전하게 저장.
// - 경로 정화(safeJoin)로 경로 이탈 방지
// - 부모 디렉터리 자동 생성
// - 임시 파일에 쓰고 rename 원자적 교체
func WriteFileEntry(baseDir string, e FileEntry) (string, error) {
	if e.FileName == "" {
		return "", fmt.Errorf("empty filename")
	}
	dst, err := safeJoin(baseDir, e.FileName)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return "", fmt.Errorf("createtemp: %w", err)
	}
	// cleanup 보장
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	if _, err := tmp.Write(e.Data); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}
	if err := tmp.Sync(); err != nil { // 디스크 플러시
		return "", fmt.Errorf("sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close: %w", err)
	}

	// Windows 대상이 존재하면 Rename 실패할 수 있으므로 한 번 지우기 시도
	_ = os.Remove(dst)
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", fmt.Errorf("rename: %w", err)
	}
	// 권한(필요 시 조정)
	if err := os.Chmod(dst, 0o644); err != nil {
		// 권한 설정 실패는 치명적이지 않다면 경고로만 처리 가능
	}

	return dst, nil
}

func publishCrash(body string) {

	// ---- Connect ----
	conn, err := amqp.Dial(amqpUrl)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)
	log.Printf("Connected to %s", amqpUrl)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	// 필요 시 큐 선언(운영에선 미리 준비되어 있으면 생략 가능)
	if declare {
		_, err = ch.QueueDeclare(
			queue, // name
			true,  // durable
			false, // autoDelete
			false, // exclusive
			false, // noWait
			nil,   // args
		)
		if err != nil {
			log.Fatalf("queue.declare: %v", err)
		}
	}

	// Publisher Confirms 활성화
	if err := ch.Confirm(false); err != nil {
		log.Fatalf("confirm.select: %v", err)
	}
	confirmCh := ch.NotifyPublish(make(chan amqp.Confirmation, 100))

	// mandatory 일 때 라우팅 실패(Return) 감지
	returnCh := ch.NotifyReturn(make(chan amqp.Return, 10))
	go func() {
		for r := range returnCh {
			log.Printf("[return] code=%d text=%s key=%s exchange=%s",
				r.ReplyCode, r.ReplyText, r.RoutingKey, r.Exchange)
		}
	}()

	log.Printf("Publishing %d message(s) to queue '%s'...", count, queue)
	for i := 1; i <= count; i++ {
		msg := body
		if count > 1 {
			msg = fmt.Sprintf("%s #%d", body, i)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ch.PublishWithContext(
			ctx,
			"",    // default exchange
			queue, // routing key = queue name (직접 큐로)
			true,  // mandatory: 라우팅 실패 시 Return 받기
			false, // immediate:
			amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				Body:         []byte(msg),
				MessageId:    fmt.Sprintf("%d", time.Now().UnixNano()),
				Timestamp:    time.Now(),
				AppId:        "go-publisher",
			},
		)
		cancel()
		if err != nil {
			log.Fatalf("publish: %v", err)
		}

		// 전송 확인(Confirm) 대기
		select {
		case c := <-confirmCh:
			if !c.Ack {
				log.Fatalf("nack received for deliveryTag=%d", c.DeliveryTag)
			}
			log.Printf(" [✓] published: %q", msg)
		case <-time.After(5 * time.Second):
			log.Fatal("confirm timeout")
		}
	}

	log.Println("Done.")
}

type program struct {
	log service.Logger
	srv *http.Server
}

func (p *program) Start(_ service.Service) error {
	if p.log != nil {
		_ = p.log.Info("service starting")
	}
	go p.run() // 비동기로 실제 서버 시작
	return nil
}

func (p *program) run() {
	addr := flag.String("addr", os.Getenv("UNREALCRASHRECEIVER_SERVER_ADDR"), "listen address (e.g. :8080 or 127.0.0.1:8080)")
	mode := flag.String("gin-mode", "release", "gin mode: debug|release|test")

	// gin 설정
	gin.SetMode(*mode)
	r := gin.New()
	r.Use(gin.Recovery())
	// (선택) 콘솔 실행일 때만 gin.Logger 미들웨어 켜기
	if _, inService := os.LookupEnv("KARDIANOS_IN_SERVICE"); !inService {
		r.Use(gin.Logger())
	}

	// HTTP 서버
	p.srv = &http.Server{
		Addr:              *addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Set a lower memory limit for multipart forms (default is 32 MiB)
	r.MaxMultipartMemory = maxMaxMultipartMemory

	// 라우트
	r.GET("/health", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	r.GET("/", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"hello": "world"}) })
	r.POST(os.Getenv("UNREALCRASHRECEIVER_SERVER_PATH_PREFIX"), func(c *gin.Context) {
		if c.Query("AppID") != "CrashReporter" {
			fmt.Printf("Invalid AppID\n")
			c.Status(http.StatusBadRequest)
			return
		}

		if strings.HasPrefix(c.Query("AppVersion"), "5.4.4") == false {
			fmt.Printf("Invalid AppVersion\n")
			c.Status(http.StatusBadRequest)
			return
		}

		if c.Query("AppEnvironment") != "Release" {
			fmt.Printf("Invalid AppEnvironment\n")
			c.Status(http.StatusBadRequest)
			return
		}

		if c.Query("UploadType") != "crashreports" {
			fmt.Printf("Invalid UploadType\n")
			c.Status(http.StatusBadRequest)
			return
		}

		// FString UserId = FString::Printf(TEXT("%s|%s|%s"), *FPlatformMisc::GetLoginId(), *FPlatformMisc::GetEpicAccountId(), *FPlatformMisc::GetOperatingSystemId());
		userId := c.Query("UserID")
		userIdTokens := strings.Split(userId, "|")
		if len(userIdTokens) != 3 {
			fmt.Printf("Invalid UserID\n")
			c.Status(http.StatusBadRequest)
			return
		}

		// zlib 압축 해제
		zr, err := zlib.NewReader(io.LimitReader(c.Request.Body, maxBody))
		if err != nil {
			fmt.Printf("Invalid zlib data\n")
			c.Status(http.StatusBadRequest)
			return
		}
		defer func(zr io.ReadCloser) {
			_ = zr.Close()
		}(zr)

		// 헤더 파싱
		br := bufio.NewReader(zr)
		h, err := ReadCR1(br, binary.LittleEndian)
		if err != nil {
			fmt.Printf("Invalid header\n")
			c.Status(http.StatusBadRequest)
			return
		}

		// 헤더 파싱은 했지만 값이 이상하다면 아웃
		if h.FileCount == 0 || len(h.FileName) == 0 || len(h.DirectoryName) == 0 || h.UncompressedSize == 0 {
			fmt.Printf("Invalid header data\n")
			c.Status(http.StatusBadRequest)
			return
		}

		// 올바른 헤더 상태에서만 출력
		fmt.Printf("DirectoryName    = %q\n", h.DirectoryName)
		fmt.Printf("FileName         = %q\n", h.FileName)
		fmt.Printf("UncompressedSize = %d\n", h.UncompressedSize)
		fmt.Printf("FileCount        = %d\n", h.FileCount)

		// 각 파일별 저장
		baseDir := "Crashes/" + h.DirectoryName
		err = os.MkdirAll(baseDir, 0o755)
		if err != nil {
			fmt.Printf("MkdirAll failed:" + err.Error())
			c.Status(http.StatusInternalServerError)
			return
		}
		for i := 0; i < int(h.FileCount); i++ {
			entry, err := ReadFileEntry(br, binary.LittleEndian)
			if err != nil {
				fmt.Printf("ReadFileEntry failed:" + err.Error())
				c.Status(http.StatusBadRequest)
				return
			}

			fileEntryPath, err := WriteFileEntry(baseDir, *entry)
			if err != nil {
				fmt.Printf("WriteFileEntry failed:" + err.Error())
				c.Status(http.StatusInternalServerError)
				return
			}

			fmt.Printf(" - %q written.\n", fileEntryPath)

			if entry.FileName == "CrashContext.runtime-xml" {
				crashXml, err := ReadCrashXML(fileEntryPath)
				if err != nil {
					fmt.Printf("ReadCrashXML failed:" + err.Error())
					c.Status(http.StatusInternalServerError)
					return
				}

				err = SendTelegramMessage(os.Getenv("UNREALCRASHRECEIVER_BOT_TOKEN"), os.Getenv("UNREALCRASHRECEIVER_CHAT_ID"), fmt.Sprintf("<b>⚠ CRASH on Windows (Build Number: %s)</b>\n%s", crashXml.GameData.RipperBuildNumber, h.DirectoryName))
				if err != nil {
					fmt.Printf("SendTelegramMessage failed:" + err.Error())
					c.Status(http.StatusInternalServerError)
					return
				}

				publishCrash(h.DirectoryName)
			}
		}

		c.Status(http.StatusOK)
	})

	// 리스닝 시작
	if p.log != nil {
		_ = p.log.Infof("listening on %s (gin-mode=%s)", *addr, *mode)
	}
	if err := p.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		if p.log != nil {
			_ = p.log.Errorf("ListenAndServe error: %v", err)
		}
	}
}

func (p *program) Stop(_ service.Service) error {
	// 서비스 Stop(또는 콘솔의 Ctrl+C) 시 그레이스풀 셧다운
	if p.log != nil {
		_ = p.log.Info("service stop requested")
	}
	if p.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := p.srv.Shutdown(ctx); err != nil {
			if p.log != nil {
				_ = p.log.Errorf("server shutdown: %v", err)
			}
		} else if p.log != nil {
			_ = p.log.Info("server shutdown complete")
		}
	}
	return nil
}

var (
	amqpUrl = ""
	queue   = ""
	count   = 0
	declare = false
)

func main() {
	goDotErr := godotenv.Load()
	if goDotErr != nil {
		panic(errors.New("error loading .env file"))
	}

	flag.Parse()

	// ---- CLI ----
	amqpUrl = *flag.String("url", os.Getenv("AMQP_URL"), "AMQP URL (amqp(s)://user:pass@host:port/vhost)")
	queue = *flag.String("queue", "crash_queue", "Queue name to publish to")
	count = *flag.Int("n", 1, "Number of messages to publish")
	declare = *flag.Bool("declare", false, "Declare the queue if not existing (durable)")

	cfg := &service.Config{
		Name:        "UnrealCrashReceiver",                               // 서비스 내부 이름 (영문/고유)
		DisplayName: "Unreal Minidump Crash Receiver for Ripper Service", // 서비스 표시 이름
		Description: "Always-on worker written in Go.",
		// Windows 전용 옵션들
		Option: map[string]interface{}{
			"StartType":        "automatic", // 부팅 시 자동 시작
			"DelayedAutoStart": true,        // 자동 시작 지연
			// "UserName": "MyDomain\\MyUser", // 특정 계정으로 실행할 때
			// "Password": "********",
		},
		EnvVars: map[string]string{"KARDIANOS_IN_SERVICE": "1"},
	}

	prg := &program{}
	svc, err := service.New(prg, cfg)
	if err != nil {
		panic(err)
	}

	logger, _ := svc.Logger(nil)
	prg.log = logger

	svcCmd := flag.String("service", "", "install | uninstall | start | stop | restart | status")

	// 서비스 관리 명령
	if *svcCmd != "" {
		switch *svcCmd {
		case "install", "uninstall", "start", "stop", "restart":
			if err := service.Control(svc, *svcCmd); err != nil {
				_ = logger.Errorf("service %s failed: %v", *svcCmd, err)
			} else {
				_ = logger.Infof("service %s ok", *svcCmd)
			}
		case "status":
			st, err := svc.Status()
			if err != nil {
				_ = logger.Errorf("status failed: %v", err)
			} else {
				fmt.Println("status:", st) // 1=stopped, 2=start pending, 4=running...
			}
		default:
			_ = logger.Errorf("unknown -service command: %s", *svcCmd)
		}
		return
	}

	// 서비스/콘솔 모드 모두 지원
	if err := svc.Run(); err != nil {
		_ = logger.Error(err)
	}
}
