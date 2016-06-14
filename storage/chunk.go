package storage

// TODO: добавлять заголовок в начало файла
type ChunkHeader struct {
	MagicNumber uint32
	Version     uint32
	Length      uint64
}
