package aof

import (
	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/lib/logger"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func (handler *Handler) newRewriteHandler() *Handler {
	h := &Handler{}
	h.aofFilename = handler.aofFilename
	h.db = handler.tmpDBMaker()
	return h
}

// RewriteCtx holds context of an AOF rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (handler *Handler) Rewrite() error {
	ctx, err := handler.StartRewrite()
	if err != nil {
		return err
	}
	err = handler.DoRewrite(ctx)
	if err != nil {
		return err
	}

	handler.FinishRewrite(ctx)
	return nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only, please use Rewrite instead
func (handler *Handler) DoRewrite(ctx *RewriteCtx) error {
	tmpFile := ctx.tmpFile

	// load aof tmpFile
	tmpAof := handler.newRewriteHandler()
	tmpAof.LoadAof(int(ctx.fileSize))

	// rewrite aof tmpFile
	for i := 0; i < config.Properties.Databases; i++ {
		// select db
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db, 从Redis数据库里读key-value进行重写
		tmpAof.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = tmpFile.Write(cmd.ToBytes())
			}
			// 超时时间不与SET KEY VALUE一起，而是单独用一条语句记录
			if expiration != nil {
				cmd := MakeExpireCmd(key, *expiration)
				if cmd != nil {
					_, _ = tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

// StartRewrite prepares rewrite procedure
func (handler *Handler) StartRewrite() (*RewriteCtx, error) {
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	// 调用 fsync 将缓冲区中的数据落盘，防止 aof 文件不完整造成错误
	err := handler.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	// 获得当前当前当前 aof 文件大小，用于判断哪些数据是 aof 重写过程中产生的
	// handleAof 会保证每次写入完整的一条指令
	fileInfo, _ := os.Stat(handler.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		// 记录开始重写时，使用的数据库
		dbIdx: handler.currentDB,
	}, nil
}

// FinishRewrite finish rewrite procedure
func (handler *Handler) FinishRewrite(ctx *RewriteCtx) {
	handler.pausingAof.Lock() // pausing aof
	defer handler.pausingAof.Unlock()

	tmpFile := ctx.tmpFile
	// 打开线上 aof 文件并 seek 到重写开始的位置
	src, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return
	}
	defer func() {
		_ = src.Close()
	}()
	// Seek之后的数据就是重写过程中的数据
	_, err = src.Seek(ctx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return
	}

	// 写入一条 Select 命令，使 tmpAof 选中重写开始时刻线上 aof 文件选中的数据库
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}
	// 对齐数据库后就可以把重写过程中产生的数据复制到 tmpAof 文件了
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof filed failed: " + err.Error())
		return
	}

	// 使用 mv 命令用 tmpAof 代替线上 aof 文件  os.Rename移动文件
	_ = handler.aofFile.Close()
	_ = os.Rename(tmpFile.Name(), handler.aofFilename)

	// 重新打开线上 aof
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	handler.aofFile = aofFile

	// 重新写入一次 select 指令保证 aof 中的数据库与 handler.currentDB 一致
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(handler.currentDB))).ToBytes()
	_, err = handler.aofFile.Write(data)
	if err != nil {
		panic(err)
	}
}
