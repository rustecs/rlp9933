package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	maxResult     int    = 20           // Results to get
	limitPerQuery int    = 10           // Results per db request
	limitThread   int    = 2            // count threads for processing
	tmpPath       string = "images"     // tmp dir for work
	resultFile    string = "result.zip" // resulf file
	statusFile    string = "status.dat" // work status
)

type config struct {
	sync.Mutex // guard tags file
	tagFile    string
	status     *status
}

// no way to get process status with tag file, have to make one more
type status struct {
	sync.Mutex // guard tags file
	statusFile string
	position   int
	proccessed map[int]bool
}

type dbRecord struct {
	id  int
	url []byte
	//tags []byte
}

func main() {
	fmt.Println("Start ...")

	conf, err := initialConfig()
	if err != nil {
		panic(err)
	}

	startPosition := conf.status.position
	processCount := startPosition
	jobs := make(chan byte, limitThread-1)
	var wg sync.WaitGroup

Loop:
	for {
		// get data
		data2Process, err := conf.getPortion(limitPerQuery, startPosition)
		if err != nil {
			panic(err)
		}
		// no more data at db
		if len(data2Process) < 1 {
			break
		}
		startPosition += len(data2Process)

		for i := range data2Process {

			if _, ok := conf.status.proccessed[data2Process[i].id]; ok {
				continue
			}

			wg.Add(1)
			go conf.commonWorker(data2Process[i], jobs, &wg)
			processCount++
			jobs <- 1 //blocker

			// no more result needed
			if processCount >= maxResult {
				break Loop
			}
		}
	}

	// wait all workers
	fmt.Println("Wait gorutines be done...")
	wg.Wait()

	// archive all
	fmt.Println("Archivate...")
	conf.archivateFiles()

	//clear
	clearTmpFiles()

	fmt.Println("Done")
}

// get file from web and put it into dir
func (c *config) commonWorker(task dbRecord, jobs <-chan byte, wg *sync.WaitGroup) {
	fmt.Printf("start process id %d\n", task.id)
	defer wg.Done()

	ext := task.getExtension()
	err := c.downloadFile(fmt.Sprintf("%s/%d%s", tmpPath, task.id, ext), string(task.url))
	if err != nil {
		panic(err) // it's io error no reason to proceed
	}
	// add data to csv
	err = c.fixWorkAtCSV(task, ext)
	if err != nil {
		panic(err) // it's io error no reason to proceed
	}

	err = c.fixWorkAtStatus(task.id)
	if err != nil {
		panic(err) // it's io error no reason to proceed
	}
	fmt.Println("working wait")
	time.Sleep(10 * time.Second)
	fmt.Println("working wait over")

	<-jobs //deblock common cycle
}

// Get the data
func (c *config) downloadFile(filepath string, url string) error {

	resp, err := http.Get(url)
	if err != nil {
		return nil // http error to /dev/null
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

func (c *config) fixWorkAtCSV(task dbRecord, ext []byte) error {
	// сложить теги в формате $id.$ext;tag1,tag2,..., tagN
	strToAdd := fmt.Sprintf("%d%s;tagsfrom task\n", task.id, ext)
	c.Lock()
	defer c.Unlock()

	f, err := os.OpenFile(c.tagFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write([]byte(strToAdd)); err != nil {
		return err
	}

	return nil
}

func (c *config) fixWorkAtStatus(id int) error {
	strToAdd := fmt.Sprintf("%d\n", id)
	c.status.Lock()
	defer c.status.Unlock()

	f, err := os.OpenFile(c.status.statusFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write([]byte(strToAdd)); err != nil {
		return err
	}

	return nil
}

// init db get last status
func initialConfig() (*config, error) {
	conf := config{}
	// tmp dir if not exists
	err := os.MkdirAll(tmpPath, os.ModePerm)
	if err != nil {
		return &conf, err
	}
	// initial pg
	// tags file name
	conf.tagFile = fmt.Sprintf("%s/tags.csv", tmpPath)
	// status file name
	conf.status = &status{
		statusFile: fmt.Sprintf("%s/%s", tmpPath, statusFile),
		proccessed: make(map[int]bool, 100),
	}

	// load status
	conf.status.position, err = conf.getStartPosition()
	if err != nil {
		return &conf, err
	}

	return &conf, nil
}

func (c *config) getStartPosition() (int, error) {
	start := 0
	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
		return start, nil
	}

	if _, err := os.Stat(c.status.statusFile); os.IsNotExist(err) {
		return start, nil
	}

	// mostly we can set start in count records...
	// but can be some fast answers while one slow still not geted
	// so we should choose: 1. reask db from begin 2. agree to lost something 3. Something else?..
	// i choose a first option so start = 0 All the time
	f, err := os.OpenFile(c.status.statusFile, os.O_RDONLY, 0600)
	if err != nil {
		return start, nil
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		//start++  // uncomment for option 2
		value, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return start, err
		}
		c.status.proccessed[value] = true
	}

	return start, nil
}

// get part data from db
func (c *config) getPortion(count int, args ...int) ([]dbRecord, error) {
	offset := 0
	if len(args) == 1 {
		offset = args[0]
	}

	list := []dbRecord{
		{100003, []byte("https://cloudstatic.eva.ru/eva/360000-370000/366613/channel/38485185897383210.jpg")},
		{100006, []byte("https://newsland.com/static/u/nlo/news_images/347/big_347354.jpg")},
		{100008, []byte("https://newsland.com/static/u/nlo/news_images/698/big_698774.jpg")},
		{100010, []byte("https://m.sovsport.ru/data/sovsport/upload/2000-01/10/images-3718-1538226329.jpg")},
		{100012, []byte("https://newsland.com/static/u/nlo/news_images/686/big_686984.jpg")},
		{100013, []byte("https://newsland.com/static/u/u/news/2018/06/6373601.jpg")},
		{100014, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1526362618.jpg")},
		{100015, []byte("https://newsland.com/static/u/u/news/2018/08/a5a10634f6054141bf3f9c2b3949eec2.jpg")},
		{100016, []byte("https://inc-news.ru/data/inc/preview/2019-05/18/image-7773-1558197927-620x414.jpg")},
		{100017, []byte("https://cloudstatic.eva.ru/eva/110000-120000/110462/channel/25007342_133908840633572_1249115912905162752_n_46221810483523510.jpg?V")},
		{100018, []byte("http://newsland.com/static/u/u/news/2019/12/bf6c8207a23c4d5f89ffc28c2a43d837.jpg")},
		{100020, []byte("https://m.sovsport.ru/data/sovsport/upload/2019-04/09/image-2847-1554806530.jpg")},
		{100021, []byte("https://m.sovsport.ru/data/sovsport/preview/2018-10/14/image-9457-1539523026-620x462.jpg")},
		{100022, []byte("https://newsland.com/static/u/nlo/news_images/1100/big_1100467.jpg")},
		{100023, []byte("https://inc-news.ru/data/inc/preview/2020-01/16/image-3377-1579172297-620x414.jpeg")},
		{100024, []byte("https://newsland.com/static/u/nlo/news_images/310/big_310490.jpg")},
		{100025, []byte("https://inc-news.ru/data/inc/preview/2020-01/07/image-6024-1578429946-620x349.jpg")},
		{100028, []byte("https://cloudstatic.eva.ru/eva/220000-230000/220612/channel/29400996_2040648329295784_5218289388085051392_n_2208247396370865.jpg?H")},
		{100029, []byte("https://newsland.com/static/u/u/news/2019/11/e4ed09883693405591dca785aa50c4c8.jpg")},
		{100030, []byte("https://cloudstatic.eva.ru/eva/720000-730000/722554/channel/67124529_118938136129173_4118601000773607244_n_7003545238127770.jpg?V")},
		{100031, []byte("https://newsland.com/static/u/u/news/2018/06/6388077.jpg")},
		{100032, []byte("https://newsland.com/static/u/nlo/news_images/613/big_613113.gif")},
		{100036, []byte("https://newsland.com/static/u/nlo/news_images/576/big_576759.jpg")},
		{100037, []byte("https://newsland.com/static/u/u/news/2016/12/7fb2f33f73ca4e47a049387b382c520b.jpg")},
		{100038, []byte("https://newsland.com/static/u/u/news/2017/04/5766969.jpg")},
		{100039, []byte("http://newsland.com/static/u/u/news/2016/11/00e9b2fe9f3a43d59712c6c13cfcaff6.jpg")},
		{100040, []byte("https://www.sovsport.ru/data/sovsport/upload/2000-01/14/images-8234-1538096486.jpg")},
		{100041, []byte("https://m.sovsport.ru/data/sovsport/preview/2019-03/05/image-5625-1551782952-620x401.jpg")},
		{100042, []byte("https://newsland.com/static/u/nlo/news_images/1491/big_1491429.jpg")},
		{100043, []byte("https://inc-news.ru/data/inc/preview/2019-08/29/image-1424-1567057017-620x414.jpg")},
		{100044, []byte("https://inc-news.ru/data/inc/preview/2020-02/01/image-3887-1580534171-620x394.jpg")},
		{100045, []byte("https://www.sovsport.ru/data/sovsport/preview/2019-09/28/image-3450-1569671370-620x469.jpg")},
		{100046, []byte("https://newsland.com/static/u/nlo/news_images/871/big_871186.jpg")},
		{100047, []byte("https://newsland.com/static/u/u/news/2019/03/45c25d865d7e445d931448ff37936ee5.jpg")},
		{100050, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1477156760.jpg")},
		{100052, []byte("https://m.sovsport.ru/data/sovsport/preview/2000-01/15/images-1718-1538233270-620x414.jpg")},
		{100053, []byte("https://newsland.com/static/u/u/news/2020/02/eb96e108ee9c4c66b9d7892212aa75f9.jpg")},
		{100054, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1517312277.jpeg")},
		{100055, []byte("https://www.sovsport.ru/data/sovsport/preview/2000-01/11/images-5075-1538219695-620x391.jpg")},
		{100056, []byte("https://newsland.com/static/u/u/news/2019/05/36faa21652104cbaaac7ec250c9f1ed1.jpg")},
		{100057, []byte("https://www.sovsport.ru/data/sovsport/upload/2000-01/11/images-4289-1538112101.jpg")},
		{100059, []byte("https://www.sovsport.ru/data/sovsport/preview/2000-01/11/images-2727-1538090714-620x465.jpg")},
		{100066, []byte("https://newsland.com/static/u/u/news/2019/11/79ccb7bbcb574c81b0f777818c7ed54c.jpg")},
		{100067, []byte("https://newsland.com/static/u/u/news/2019/04/5fa5db39b8a940ecaa7faf4a07205327.jpg")},
		{100069, []byte("https://www.sovsport.ru/data/sovsport/upload/2000-01/11/images-9817-1538109712.jpg")},
		{100071, []byte("https://inc-news.ru/data/inc/preview/2020-02/01/image-1295-1580536811-620x349.jpg")},
		{100073, []byte("https://inc-news.ru/data/inc/preview/2019-10/09/image-3853-1570570257-620x414.jpg")},
		{100075, []byte("https://newsland.com/static/u/u/news/2019/12/93bd3e1e92c1493998ff42086f8eabc1.jpg")},
		{100076, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1544467554.jpg")},
		{100081, []byte("https://m.sovsport.ru/data/sovsport/preview/2000-01/10/images-2610-1538226315-620x439.jpg")},
		{100082, []byte("https://www.paparazzi.ru/upload/medialibrary/63c/63c10a19ec6f1d3d7b35e38666c47219.jpg")},
		{100084, []byte("http://newsland.com/static/u/u/news/2020/02/a6dc33ccb4254d60a88f4bfc656c5350.png")},
		{100085, []byte("https://www.sovsport.ru/data/sovsport/upload/2000-01/17/images-1791-1538140184.jpg")},
		{100087, []byte("https://newsland.com/static/u/nlo/news_images/1296/big_1296629.jpg")},
		{100088, []byte("https://newsland.com/static/u/nlo/news_images/1017/big_1017242.jpeg")},
		{100090, []byte("https://newsland.com/static/u/u/news/2018/07/6406220.jpg")},
		{100093, []byte("http://newsland.com/static/u/u/news/2017/06/e6d2c9b1b6a34f9bb3c9b5098f75fe07.jpg")},
		{100094, []byte("https://newsland.com/static/u/nlo/news_images/886/big_886456.jpg")},
		{100095, []byte("https://cloudstatic.eva.ru/eva/530000-540000/535260/channel/24177527_150473218915501_7870963178388062208_n_588331967066396.jpg?H")},
		{100096, []byte("https://newsland.com/static/u/u/news/2018/08/1a850ef89fc345e28f52443aab60f3ce.jpg")},
		{100098, []byte("http://newsland.com/static/u/u/news/2019/07/38636f54c2514a959971145e400c896a.jpg")},
		{100101, []byte("https://newsland.com/static/u/nlo/news_images/358/big_358998.jpg")},
		{100102, []byte("https://newsland.com/static/u/nlo/news_images/1676/big_1676647.jpg")},
		{100104, []byte("https://newsland.com/static/u/nlo/news_images/561/big_561145.jpg")},
		{100105, []byte("https://newsland.com/static/u/u/news/2018/06/24627c00cdef49619c3f610ec77a3612.jpg")},
		{100107, []byte("https://newsland.com/static/u/u/news/2019/05/ccad0bd49c95438988873ef87b2552ba.jpg")},
		{100108, []byte("https://m.sovsport.ru/data/sovsport/preview/2019-10/19/image-3870-1571496182-620x414.jpg")},
		{100111, []byte("https://www.sovsport.ru/data/sovsport/upload/2000-01/11/images-5676-1538103758.jpg")},
		{100112, []byte("https://newsland.com/static/u/u/news/2018/10/e85f380e9d70471eadba229edde62a29.jpg")},
		{100113, []byte("https://newsland.com/static/u/nlo/news_images/974/big_974356.jpg")},
		{100114, []byte("https://newsland.com/static/u/nlo/news_images/1448/big_1448246.jpg")},
		{100116, []byte("http://newsland.com/static/u/u/news/2017/03/5710521.jpg")},
		{100117, []byte("https://newsland.com/static/u/nlo/news_images/639/big_639989.jpg")},
		{100118, []byte("https://newsland.com/static/u/nlo/news_images/926/big_926742.jpg")},
		{100119, []byte("https://newsland.com/static/u/nlo/news_images/1232/big_1232096.jpg")},
		{100122, []byte("https://newsland.com/static/u/nlo/news_images/1399/big_1399376.jpg")},
		{100123, []byte("https://newsland.com/static/u/nlo/news_images/1406/big_1406888.jpg")},
		{100125, []byte("https://newsland.com/static/u/nlo/news_images/566/big_566298.jpg")},
		{100126, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1568614572.jpg")},
		{100128, []byte("https://www.sovsport.ru/data/sovsport/preview/2000-01/18/images-8316-1538137074-620x345.jpg")},
		{100129, []byte("https://www.paparazzi.ru/upload/wysiwyg_files/img/1579266268.jpg")},
		{100130, []byte("https://cloudstatic.eva.ru/eva/520000-530000/520008/channel/26340449_158780534844051_1521005184036110336_n_50273171971834293.jpg?H")},
		{100134, []byte("https://cloudstatic.eva.ru/eva/520000-530000/526792/channel/21909944_971817249624944_8500749952580845568_n_40062879861652005.jpg?H")},
		{100136, []byte("https://newsland.com/static/u/nlo/news_images/1555/big_15552891433672290.jpg")},
		{100140, []byte("https://newsland.com/static/u/nlo/news_images/631/big_631309.jpg")},
		{100141, []byte("https://cloudstatic.eva.ru/eva/520000-530000/528809/channel/2_36979303773539812.jpg?V")},
		{100142, []byte("https://www.paparazzi.ru/upload/medialibrary/ee4/ee4ab22c81685dfe3aaadeadf1e12ac6.jpg")},
		{100144, []byte("https://www.paparazzi.ru/upload/medialibrary/d9d/d9db391885ae14f1f8e1ad4a99090747.jpg")},
		{100146, []byte("https://m.sovsport.ru/data/sovsport/preview/2000-01/15/images-6286-1538133863-620x349.jpg")},
		{100147, []byte("https://newsland.com/static/u/nlo/news_images/559/big_559817.jpg")},
		{100148, []byte("https://www.sovsport.ru/data/sovsport/preview/2020-02/02/image-6228-1580658176-620x414.jpg")},
		{100150, []byte("http://zvezdi.ru/uploads/posts/2019-07/1563808255_mariya-roshe-marie-rauscher-v-pole-playboy-germaniya-iyul-2019-1.jpg")},
		{100151, []byte("https://newsland.com/static/u/u/news/2018/05/d9569f7dedc140d88b0cd1d67ef031d5.jpg")},
		{100152, []byte("https://cloudstatic.eva.ru/eva/90000-100000/93759/channel/---2_6750878182233688.jpg?V")},
		{100153, []byte("http://www.paparazzi.ru/upload/blogimages/133/133f6d3fe4ed45ec97522964578b02d5.jpg")},
		{100154, []byte("https://newsland.com/static/u/u/news/2019/11/cddac201a0084d52ac2a4e7809dbbe5c.jpg")},
		{100156, []byte("https://newsland.com/static/u/u/news/2017/06/5868945.jpg")},
		{100157, []byte("https://newsland.com/static/u/nlo/news_images/344/big_344006.jpg")},
		{100158, []byte("https://newsland.com/static/u/nlo/news_images/1392/big_1392919.jpg")},
		{100160, []byte("https://newsland.com/static/u/u/news/2017/03/5724735.jpg")},
	}

	out := make([]dbRecord, count)

	switch {
	case len(list) < offset:
		// do nothing
	case len(list) < offset+count:
		out = list[offset:]
	default:
		out = list[offset : offset+count]
	}

	return out, nil
}

func (t *dbRecord) getExtension() []byte {

	pos := len(t.url)
	paramIndex := bytes.IndexRune(t.url, '?')
	if paramIndex > -1 {
		pos = paramIndex
	}

	noparam := t.url[0:pos]
	point := bytes.LastIndexByte(noparam, '.')
	if point > -1 {
		return noparam[point:]
	}

	return []byte{}
}

func (c *config) archivateFiles() {

	// rename old of exists
	if _, err := os.Stat(resultFile); !os.IsNotExist(err) {
		os.Rename(resultFile, fmt.Sprintf("%s.%d", resultFile, time.Now().Unix()))
	}

	archive, err := os.Create(resultFile)
	if err != nil {
		panic(err)
	}
	defer archive.Close()

	zipWriter := zip.NewWriter(archive)
	defer zipWriter.Close()

	entries, err := os.ReadDir(tmpPath)
	if err != nil {
		panic(err)
	}

	for _, file := range entries {
		fileName := fmt.Sprintf("%s/%s", tmpPath, file.Name())

		// skip internal data
		if fileName == c.status.statusFile {
			continue
		}

		source, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}

		/*
			результатом работы является архивный файл структуры
			images/N.jpg
			....
			images/M.jpg
			tags.csv   <--- not a common path!
		*/
		if fileName == c.tagFile {
			fileName = file.Name()
		}
		target, err := zipWriter.Create(fileName)
		if err != nil {
			panic(err)
		}

		if _, err := io.Copy(target, source); err != nil {
			panic(err)
		}

		source.Close()
	}
}

func clearTmpFiles() {
	err := os.RemoveAll(tmpPath)
	if err != nil {
		panic(err)
	}
}
