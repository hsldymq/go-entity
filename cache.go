package entity

import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

// DefaultCacher 默认缓存存储
var DefaultCacher Cacher

// DefaultEnDecoder 默认的缓存数据编码解码器
var DefaultEnDecoder = &JSONEnDecoder{}

// DefaultExpiration 默认缓存时间
var DefaultExpiration = 5 * time.Minute

// Cacheable 可缓存实体对象接口
type Cacheable interface {
	CacheOption() *CacheOption
}

// Cacher 缓存数据存储接口
type Cacher interface {
	Get(key string) ([]byte, error)
	Put(key string, data []byte, expiration time.Duration) error
	Delete(key string) error
}

// Encoder 编码对象,转换可缓存的字节数组
type Encoder interface {
	Encode(Cacheable) ([]byte, error)
}

// Decoder 解码缓存数据,将缓存还原到entity中
type Decoder interface {
	Decode([]byte, Cacheable) error
}

// JSONEnDecoder 默认缓存数据编码解码器
type JSONEnDecoder struct{}

// Encode 编码
func (e *JSONEnDecoder) Encode(c Cacheable) ([]byte, error) {
	data, err := jsoniter.Marshal(c)
	return data, errors.WithStack(err)
}

// Decode 解码
func (e *JSONEnDecoder) Decode(data []byte, c Cacheable) error {
	err := jsoniter.Unmarshal(data, c)
	return errors.WithStack(err)
}

// CacheOption 缓存参数
type CacheOption struct {
	Cacher     Cacher
	Key        string
	Expiration time.Duration
	Encoder    Encoder
	Decoder    Decoder
}

func loadCache(c Cacheable) (bool, error) {
	opt, err := getCacheOption(c)
	if err != nil {
		return false, err
	}

	data, err := opt.Cacher.Get(opt.Key)
	if err != nil {
		return false, errors.WithStack(err)
	} else if len(data) == 0 {
		return false, nil
	}

	if opt.Decoder == nil {
		err = DefaultEnDecoder.Decode(data, c)
	} else {
		err = errors.WithStack(opt.Decoder.Decode(data, c))
	}

	return err == nil, err
}

// SaveCache 保存entity缓存
func SaveCache(c Cacheable) error {
	opt, err := getCacheOption(c)
	if err != nil {
		return err
	}

	var data []byte
	if opt.Encoder == nil {
		data, err = DefaultEnDecoder.Encode(c)
	} else {
		data, err = opt.Encoder.Encode(c)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.Wrap(opt.Cacher.Put(opt.Key, data, opt.Expiration), "save entity cache")
}

// DeleteCache 删除entity缓存
func DeleteCache(c Cacheable) error {
	opt, err := getCacheOption(c)
	if err != nil {
		return err
	}

	return errors.Wrap(opt.Cacher.Delete(opt.Key), "delete entity cache")
}

func getCacheOption(c Cacheable) (*CacheOption, error) {
	opt := c.CacheOption()

	if opt.Cacher == nil {
		if DefaultCacher == nil {
			return nil, errors.New("require cacher")
		}

		opt.Cacher = DefaultCacher
	}

	if opt.Key == "" {
		return nil, errors.New("empty cache key")
	}

	if opt.Expiration == 0 {
		opt.Expiration = DefaultExpiration
	}

	return opt, nil
}
