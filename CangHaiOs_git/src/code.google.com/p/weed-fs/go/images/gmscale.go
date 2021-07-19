// +build gm

package images

/*
#cgo CFLAGS: -I /usr/local/include
#cgo CFLAGS: -O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector --param=ssp-buffer-size=4 -m64 -mtune=generic -Wall -pthread
#cgo LDFLAGS: -L /usr/lib64 -L/usr/local/lib -L/usr/lib
#cgo LDFLAGS: -lavdevice -lavformat -lavfilter -lavcodec -lswresample -lswscale -lavutil  -ldl -lswscale  -lm -lrt -lpthread -lz -ldl
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavfilter/avfiltergraph.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libavutil/opt.h>
#include <libavutil/pixdesc.h>

#define DEFAULT_MEM (10*1024*1024)

typedef struct FilteringContext {
    AVFilterContext *buffersink_ctx;
    AVFilterContext *buffersrc_ctx;
    AVFilterGraph *filter_graph;
} FilteringContext;

typedef struct StreamContext {
    AVCodecContext *dec_ctx;
    AVCodecContext *enc_ctx;
} StreamContext;

typedef struct AVIOBufferContext {
    unsigned char* ptr;
    int pos;
    int totalSize;
    int realSize;
}AVIOBufferContext;
typedef struct ScaleCxt {
    FilteringContext *filter_ctx;
	StreamContext *stream_ctx;
	AVFormatContext *ifmt_ctx;
	AVFormatContext *ofmt_ctx;
}ScaleCxt;


static int read_buffer(void *opaque, unsigned char *buf, int size)
{   //printf("%s %s  %s:%d   new read buf size is %d\n",__DATE__,__TIME__,__FILE__,__LINE__,size);
    AVIOBufferContext* op = (AVIOBufferContext*)opaque;
	//printf("%s %s  %s:%d ptr %p    totalSize %d   realsize %d  pos %d\n",__DATE__,__TIME__,__FILE__,__LINE__,op->ptr,op->totalSize,op->realSize,op->pos);
    int len = size;
    if (op->pos + size > op->totalSize)
    {
        len = op->totalSize - op->pos;
    }
	if (len<=0)
		return -1;

    memcpy(buf, op->ptr + op->pos, len);
    if (op->pos + len >= op->realSize)
    op->realSize += len;

    op->pos += len;

    return len;
}

static int write_buffer(void *opaque, unsigned char *buf, int size)
{
    AVIOBufferContext* op = (AVIOBufferContext*)opaque;
    if (op->pos + size > op->totalSize)
    {
        int newTotalLen = op->totalSize*sizeof(char) * 3 / 2;
        unsigned char* ptr = (unsigned char*)av_realloc(op->ptr, newTotalLen);
        if (ptr == NULL)
        {
            return -1;
        }
        op->totalSize = newTotalLen;
        op->ptr = ptr;
    }
    memcpy(op->ptr + op->pos, buf, size);
    if (op->pos + size >= op->realSize)
        op->realSize += size;
    op->pos += size;
    return 0;
}


static int open_input_file(AVIOBufferContext *bd,ScaleCxt * psctx)
{
    int ret;
    unsigned int i;
    psctx->ifmt_ctx = NULL;
	psctx->ifmt_ctx = avformat_alloc_context();

    if (!psctx->ifmt_ctx) {
		printf("%s %s  %s:%d   Could not create output context\n",__DATE__,__TIME__,__FILE__,__LINE__);
        ret = AVERROR_UNKNOWN;
        return ret;
    }
     unsigned char* inbuffer=(unsigned char*)av_malloc(32768);
	 if (inbuffer==NULL)
	 {
		 return -1;
	 }
	 AVIOContext *avio_in = avio_alloc_context(inbuffer, 32768,0, bd, read_buffer, NULL, NULL);
    if (!avio_in)
        return -1;
	psctx->ifmt_ctx->pb=avio_in;
	psctx->ifmt_ctx->flags=AVFMT_FLAG_CUSTOM_IO;
	  if ((ret = avformat_open_input(&psctx->ifmt_ctx, NULL, NULL, NULL)) < 0) {
		printf("%s %s  %s:%d   Cannot open input file\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return ret;
    }


    if ((ret = avformat_find_stream_info(psctx->ifmt_ctx, NULL)) < 0) {
		printf("%s %s  %s:%d   Cannot find stream information\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return ret;
    }

    psctx->stream_ctx = av_mallocz_array(psctx->ifmt_ctx->nb_streams, sizeof(*psctx->stream_ctx));
    if (!psctx->stream_ctx)
        return AVERROR(ENOMEM);

    for (i = 0; i < psctx->ifmt_ctx->nb_streams; i++) {
        AVStream *stream = psctx->ifmt_ctx->streams[i];
        AVCodec *dec = avcodec_find_decoder(stream->codecpar->codec_id);
        AVCodecContext *codec_ctx;
        if (!dec) {
			printf("%s %s  %s:%d   Failed to find decoder for stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
            return AVERROR_DECODER_NOT_FOUND;
        }
        codec_ctx = avcodec_alloc_context3(dec);
        if (!codec_ctx) {
			printf("%s %s  %s:%d   Failed to allocate the decoder context for stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
            return AVERROR(ENOMEM);
        }
        ret = avcodec_parameters_to_context(codec_ctx, stream->codecpar);
        if (ret < 0) {
			printf("%s %s  %s:%d   Failed to copy decoder parameters to input decoder context for stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
            return ret;
        }
        if (codec_ctx->codec_type == AVMEDIA_TYPE_VIDEO
                || codec_ctx->codec_type == AVMEDIA_TYPE_AUDIO) {
            if (codec_ctx->codec_type == AVMEDIA_TYPE_VIDEO)
                codec_ctx->framerate = av_guess_frame_rate(psctx->ifmt_ctx, stream, NULL);
            ret = avcodec_open2(codec_ctx, dec, NULL);
            if (ret < 0) {
				printf("%s %s  %s:%d   Failed to open decoder for stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
                return ret;
            }
        }
        psctx->stream_ctx[i].dec_ctx = codec_ctx;
    }

    //av_dump_format(psctx->ifmt_ctx, 0, "", 0);
    return 0;
}

static int open_output_file(int qscale,AVIOBufferContext * dataout,ScaleCxt * psctx)
{
    AVStream *out_stream;
    AVStream *in_stream;
    AVCodecContext *dec_ctx, *enc_ctx;
    AVCodec *encoder;
    int ret;
    unsigned int i;

    psctx->ofmt_ctx = NULL;

	avformat_alloc_output_context2(&psctx->ofmt_ctx, NULL, "mjpeg", NULL);
    if (!psctx->ofmt_ctx) {
		printf("%s %s  %s:%d   Could not create output context\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return AVERROR_UNKNOWN;
    }
	unsigned char* outbuffer=(unsigned char*)av_malloc(32768);
	if (outbuffer==NULL)
	 {
		 return -1;
	 }
	AVIOContext *avio_out = avio_alloc_context(outbuffer, 32768,1, dataout, NULL, write_buffer, NULL);
    if (!avio_out)
        return -1;

	psctx->ofmt_ctx->pb=avio_out;
	psctx->ofmt_ctx->flags=AVFMT_FLAG_CUSTOM_IO;
    for (i = 0; i < psctx->ifmt_ctx->nb_streams; i++) {
        out_stream = avformat_new_stream(psctx->ofmt_ctx, NULL);
        if (!out_stream) {
			printf("%s %s  %s:%d   Failed allocating output stream\n",__DATE__,__TIME__,__FILE__,__LINE__);
            return AVERROR_UNKNOWN;
        }

        in_stream = psctx->ifmt_ctx->streams[i];
        dec_ctx = psctx->stream_ctx[i].dec_ctx;

        if (dec_ctx->codec_type == AVMEDIA_TYPE_VIDEO
                || dec_ctx->codec_type == AVMEDIA_TYPE_AUDIO) {

            encoder = avcodec_find_encoder(dec_ctx->codec_id);
            if (!encoder) {
				printf("%s %s  %s:%d   Necessary encoder not found\n",__DATE__,__TIME__,__FILE__,__LINE__);
                return AVERROR_INVALIDDATA;
            }
            enc_ctx = avcodec_alloc_context3(encoder);
            if (!enc_ctx) {
				printf("%s %s  %s:%d   Failed to allocate the encoder context\n",__DATE__,__TIME__,__FILE__,__LINE__);
                return AVERROR(ENOMEM);
            }


            if (dec_ctx->codec_type == AVMEDIA_TYPE_VIDEO) {
                enc_ctx->height = dec_ctx->height;
                enc_ctx->width = dec_ctx->width;
                enc_ctx->sample_aspect_ratio = dec_ctx->sample_aspect_ratio;

                if (encoder->pix_fmts)
                    enc_ctx->pix_fmt = encoder->pix_fmts[0];
                else
                    enc_ctx->pix_fmt = dec_ctx->pix_fmt;

                enc_ctx->time_base = av_inv_q(dec_ctx->framerate);
				if (qscale>0){
					enc_ctx->flags |= AV_CODEC_FLAG_QSCALE;
					enc_ctx->global_quality = FF_QP2LAMBDA * qscale;
				}


            } else {
                enc_ctx->sample_rate = dec_ctx->sample_rate;
                enc_ctx->channel_layout = dec_ctx->channel_layout;
                enc_ctx->channels = av_get_channel_layout_nb_channels(enc_ctx->channel_layout);

                enc_ctx->sample_fmt = encoder->sample_fmts[0];
                enc_ctx->time_base = (AVRational){1, enc_ctx->sample_rate};
            }



            ret = avcodec_open2(enc_ctx, encoder, NULL);
            if (ret < 0) {
				printf("%s %s  %s:%d   Cannot open video encoder for stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
                return ret;
            }
            ret = avcodec_parameters_from_context(out_stream->codecpar, enc_ctx);
            if (ret < 0) {
				printf("%s %s  %s:%d   Failed to copy encoder parameters to output stream #%u\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
                return ret;
            }
            if (psctx->ofmt_ctx->oformat->flags & AVFMT_GLOBALHEADER)
                enc_ctx->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;

            out_stream->time_base = enc_ctx->time_base;
            psctx->stream_ctx[i].enc_ctx = enc_ctx;
        } else if (dec_ctx->codec_type == AVMEDIA_TYPE_UNKNOWN) {
			printf("%s %s  %s:%d   Elementary stream #%d is of unknown type, cannot proceed\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
            return AVERROR_INVALIDDATA;
        } else {

            ret = avcodec_parameters_copy(out_stream->codecpar, in_stream->codecpar);
            if (ret < 0) {
				printf("%s %s  %s:%d   Copying parameters for stream #%u failed\n",__DATE__,__TIME__,__FILE__,__LINE__,i);
                return ret;
            }
            out_stream->time_base = in_stream->time_base;
        }

    }
	//av_dump_format(psctx->ofmt_ctx, 0, "", 1);


    ret = avformat_write_header(psctx->ofmt_ctx, NULL);
    if (ret < 0) {
		printf("%s %s  %s:%d   Error occurred when opening output file\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return ret;
    }

    return 0;
}


static int init_filter(FilteringContext* fctx, AVCodecContext *dec_ctx,
        AVCodecContext *enc_ctx, const char *filter_spec)
{
    char args[512];
    int ret = 0;
    AVFilter *buffersrc = NULL;
    AVFilter *buffersink = NULL;
    AVFilterContext *buffersrc_ctx = NULL;
    AVFilterContext *buffersink_ctx = NULL;
    AVFilterInOut *outputs = avfilter_inout_alloc();
    AVFilterInOut *inputs  = avfilter_inout_alloc();
    AVFilterGraph *filter_graph = avfilter_graph_alloc();

    if (!outputs || !inputs || !filter_graph) {
        ret = AVERROR(ENOMEM);
        goto end;
    }

    if (dec_ctx->codec_type == AVMEDIA_TYPE_VIDEO) {
        buffersrc = avfilter_get_by_name("buffer");
        buffersink = avfilter_get_by_name("buffersink");
        if (!buffersrc || !buffersink) {
			printf("%s %s  %s:%d   filtering source or sink element not found\n",__DATE__,__TIME__,__FILE__,__LINE__);
            ret = AVERROR_UNKNOWN;
            goto end;
        }

        snprintf(args, sizeof(args),
                "video_size=%dx%d:pix_fmt=%d:time_base=%d/%d:pixel_aspect=%d/%d",
                dec_ctx->width, dec_ctx->height, dec_ctx->pix_fmt,
                dec_ctx->time_base.num, dec_ctx->time_base.den,
                dec_ctx->sample_aspect_ratio.num,
                dec_ctx->sample_aspect_ratio.den);
		filter_graph->nb_threads=1;
        ret = avfilter_graph_create_filter(&buffersrc_ctx, buffersrc, "in",
                args, NULL, filter_graph);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot create buffer source\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = avfilter_graph_create_filter(&buffersink_ctx, buffersink, "out",
                NULL, NULL, filter_graph);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot create buffer sink\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = av_opt_set_bin(buffersink_ctx, "pix_fmts",
                (uint8_t*)&enc_ctx->pix_fmt, sizeof(enc_ctx->pix_fmt),
                AV_OPT_SEARCH_CHILDREN);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot set output pixel format\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }
    } else if (dec_ctx->codec_type == AVMEDIA_TYPE_AUDIO) {
        buffersrc = avfilter_get_by_name("abuffer");
        buffersink = avfilter_get_by_name("abuffersink");
        if (!buffersrc || !buffersink) {
			printf("%s %s  %s:%d   filtering source or sink element not found\n",__DATE__,__TIME__,__FILE__,__LINE__);
            ret = AVERROR_UNKNOWN;
            goto end;
        }

        if (!dec_ctx->channel_layout)
            dec_ctx->channel_layout =
                av_get_default_channel_layout(dec_ctx->channels);
        snprintf(args, sizeof(args),
                "time_base=%d/%d:sample_rate=%d:sample_fmt=%s:channel_layout=0x%"PRIx64,
                dec_ctx->time_base.num, dec_ctx->time_base.den, dec_ctx->sample_rate,
                av_get_sample_fmt_name(dec_ctx->sample_fmt),
                dec_ctx->channel_layout);
        ret = avfilter_graph_create_filter(&buffersrc_ctx, buffersrc, "in",
                args, NULL, filter_graph);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot create audio buffer source\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = avfilter_graph_create_filter(&buffersink_ctx, buffersink, "out",
                NULL, NULL, filter_graph);
        if (ret < 0) {
			printf("%s %s  %s:%d  Cannot create audio buffer sink\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = av_opt_set_bin(buffersink_ctx, "sample_fmts",
                (uint8_t*)&enc_ctx->sample_fmt, sizeof(enc_ctx->sample_fmt),
                AV_OPT_SEARCH_CHILDREN);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot set output sample format\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = av_opt_set_bin(buffersink_ctx, "channel_layouts",
                (uint8_t*)&enc_ctx->channel_layout,
                sizeof(enc_ctx->channel_layout), AV_OPT_SEARCH_CHILDREN);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot set output channel layout\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = av_opt_set_bin(buffersink_ctx, "sample_rates",
                (uint8_t*)&enc_ctx->sample_rate, sizeof(enc_ctx->sample_rate),
                AV_OPT_SEARCH_CHILDREN);
        if (ret < 0) {
			printf("%s %s  %s:%d   Cannot set output sample rate\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }
    } else {
        ret = AVERROR_UNKNOWN;
        goto end;
    }


    outputs->name       = av_strdup("in");
    outputs->filter_ctx = buffersrc_ctx;
    outputs->pad_idx    = 0;
    outputs->next       = NULL;

    inputs->name       = av_strdup("out");
    inputs->filter_ctx = buffersink_ctx;
    inputs->pad_idx    = 0;
    inputs->next       = NULL;

    if (!outputs->name || !inputs->name) {
        ret = AVERROR(ENOMEM);
        goto end;
    }

    if ((ret = avfilter_graph_parse_ptr(filter_graph, filter_spec,
                    &inputs, &outputs, NULL)) < 0)
        goto end;

    if ((ret = avfilter_graph_config(filter_graph, NULL)) < 0)
        goto end;


    fctx->buffersrc_ctx = buffersrc_ctx;
    fctx->buffersink_ctx = buffersink_ctx;
    fctx->filter_graph = filter_graph;

end:
    avfilter_inout_free(&inputs);
    avfilter_inout_free(&outputs);

    return ret;
}

static int init_filters(ScaleCxt * psctx)
{
    const char *filter_spec;
    unsigned int i;
    int ret;
    psctx->filter_ctx = av_malloc_array(psctx->ifmt_ctx->nb_streams, sizeof(*psctx->filter_ctx));
    if (!psctx->filter_ctx)
        return AVERROR(ENOMEM);

    for (i = 0; i < psctx->ifmt_ctx->nb_streams; i++) {
        psctx->filter_ctx[i].buffersrc_ctx  = NULL;
        psctx->filter_ctx[i].buffersink_ctx = NULL;
        psctx->filter_ctx[i].filter_graph   = NULL;
        if (!(psctx->ifmt_ctx->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_AUDIO
                || psctx->ifmt_ctx->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO))
            continue;


        if (psctx->ifmt_ctx->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO)
            filter_spec = "null";
        else
            filter_spec = "anull";
        ret = init_filter(&psctx->filter_ctx[i], psctx->stream_ctx[i].dec_ctx,
                psctx->stream_ctx[i].enc_ctx, filter_spec);
        if (ret)
            return ret;
    }
    return 0;
}

static int encode_write_frame(AVFrame *filt_frame, unsigned int stream_index, int *got_frame,ScaleCxt * psctx) {
    int ret;
    int got_frame_local;
    AVPacket enc_pkt;
    int (*enc_func)(AVCodecContext *, AVPacket *, const AVFrame *, int *) =
        (psctx->ifmt_ctx->streams[stream_index]->codecpar->codec_type ==
         AVMEDIA_TYPE_VIDEO) ? avcodec_encode_video2 : avcodec_encode_audio2;

    if (!got_frame)
        got_frame = &got_frame_local;

	//printf("%s %s  %s:%d   Encoding frame\n",__DATE__,__TIME__,__FILE__,__LINE__);

    enc_pkt.data = NULL;
    enc_pkt.size = 0;
    av_init_packet(&enc_pkt);
	 filt_frame->quality=psctx->stream_ctx[stream_index].enc_ctx->global_quality;
    ret = enc_func(psctx->stream_ctx[stream_index].enc_ctx, &enc_pkt,
            filt_frame, got_frame);
    av_frame_free(&filt_frame);
    if (ret < 0)
        return ret;
    if (!(*got_frame))
        return 0;


    enc_pkt.stream_index = stream_index;
    av_packet_rescale_ts(&enc_pkt,
                         psctx->stream_ctx[stream_index].enc_ctx->time_base,
                         psctx->ofmt_ctx->streams[stream_index]->time_base);

	//printf("%s %s  %s:%d   Muxing frame\n",__DATE__,__TIME__,__FILE__,__LINE__);


    ret = av_interleaved_write_frame(psctx->ofmt_ctx, &enc_pkt);
    return ret;
}

static int filter_encode_write_frame(AVFrame *frame, unsigned int stream_index,ScaleCxt * psctx)
{
    int ret;
    AVFrame *filt_frame;
	//printf("%s %s  %s:%d   Pushing decoded frame to filters\n",__DATE__,__TIME__,__FILE__,__LINE__);

    ret = av_buffersrc_add_frame_flags(psctx->filter_ctx[stream_index].buffersrc_ctx,
            frame, 0);
    if (ret < 0) {
		printf("%s %s  %s:%d   Error while feeding the filtergraph\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return ret;
    }


    while (1) {
        filt_frame = av_frame_alloc();
        if (!filt_frame) {
            ret = AVERROR(ENOMEM);
            break;
        }
		//printf("%s %s  %s:%d   Pulling filtered frame from filters\n",__DATE__,__TIME__,__FILE__,__LINE__);
        ret = av_buffersink_get_frame(psctx->filter_ctx[stream_index].buffersink_ctx,
                filt_frame);
        if (ret < 0) {

            if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF)
                ret = 0;
            av_frame_free(&filt_frame);
            break;
        }

        filt_frame->pict_type = AV_PICTURE_TYPE_NONE;
        ret = encode_write_frame(filt_frame, stream_index, NULL,psctx);
        if (ret < 0)
            break;
    }

    return ret;
}

static int flush_encoder(unsigned int stream_index,ScaleCxt * psctx)
{
    int ret;
    int got_frame;

    if (!(psctx->stream_ctx[stream_index].enc_ctx->codec->capabilities &
                AV_CODEC_CAP_DELAY))
        return 0;

    while (1) {
		printf("%s %s  %s:%d   Flushing stream #%u encoder\n",__DATE__,__TIME__,__FILE__,__LINE__,stream_index);
        ret = encode_write_frame(NULL, stream_index, &got_frame,psctx);
        if (ret < 0)
            break;
        if (!got_frame)
            return 0;
    }
    return ret;
}

void * ffprocess(int  datail,void *data,int *dataol)
{
    int ret=0;
	unsigned char* retdata=NULL;
	ScaleCxt scxt={0};
	AVIOBufferContext avbin={0},avbout={0};
	*dataol=0;
    avbin.ptr=data;
	avbin.totalSize=datail;
	avbout.ptr  = (unsigned char*)av_realloc(NULL, DEFAULT_MEM*sizeof(char));
	if (avbout.ptr  == NULL)
    {
		printf("%s %s  %s:%d   alloc output buffer mem failed.\n",__DATE__,__TIME__,__FILE__,__LINE__);
        return NULL;
    }
    avbout.totalSize = DEFAULT_MEM;

	ret=scale(&avbin,&avbout,&scxt);
	if (ret != 0 || avbout.realSize==0)
	{
		retdata=NULL;
	}
	else
	{  retdata=(unsigned char*)av_realloc(NULL, avbout.realSize*sizeof(char));
	   if (retdata==NULL) {
		   printf("%s %s  %s:%d  alloc output mem failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
	   }
	   else
	   {
		   memcpy(retdata,avbout.ptr,avbout.realSize);
		   *dataol=avbout.realSize;
	   }
	}
	if (NULL!=avbout.ptr)
	{
		av_free(avbout.ptr);
	}

	return retdata;
}



int scale(AVIOBufferContext *pin,AVIOBufferContext *pout,ScaleCxt *psctx)
{
    int ret;
    AVPacket packet = { .data = NULL, .size = 0 };
    AVFrame *frame = NULL;
    enum AVMediaType type;
    unsigned int stream_index;
    unsigned int i;
    int got_frame;
	int (*dec_func)(AVCodecContext *, AVFrame *, int *, const AVPacket *);

    if (ret = open_input_file(pin,psctx) < 0)
	{
		printf("%s %s  %s:%d  open input failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
		goto end;
	}



    if ((ret = open_output_file(4,pout,psctx)) < 0)  //4 for quality 86
    {
		printf("%s %s  %s:%d  open output  failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
		goto end;
	}

    if (ret = init_filters(psctx) < 0)
    {
		printf("%s %s  %s:%d  open output fillters failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
		goto end;
	}


    while (1) {
        if ((ret = av_read_frame(psctx->ifmt_ctx, &packet)) < 0)
            break;
        stream_index = packet.stream_index;
        type = psctx->ifmt_ctx->streams[packet.stream_index]->codecpar->codec_type;
       // printf("%s %s  %s:%d   Demuxer gave frame of stream_index %u\n",__DATE__,__TIME__,__FILE__,__LINE__,stream_index);
        if (psctx->filter_ctx[stream_index].filter_graph) {
			//printf("%s %s  %s:%d   Going to reencode&filter the frame\n",__DATE__,__TIME__,__FILE__,__LINE__);
            frame = av_frame_alloc();
            if (!frame) {
                ret = AVERROR(ENOMEM);
                break;
            }
            av_packet_rescale_ts(&packet,
                                 psctx->ifmt_ctx->streams[stream_index]->time_base,
                                 psctx->stream_ctx[stream_index].dec_ctx->time_base);
            dec_func = (type == AVMEDIA_TYPE_VIDEO) ? avcodec_decode_video2 :
                avcodec_decode_audio4;
            ret = dec_func(psctx->stream_ctx[stream_index].dec_ctx, frame,
                    &got_frame, &packet);
            if (ret < 0) {
                av_frame_free(&frame);
				printf("%s %s  %s:%d   Decoding failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
                break;
            }

            if (got_frame) {
                frame->pts = frame->best_effort_timestamp;
                ret = filter_encode_write_frame(frame, stream_index,psctx);
                av_frame_free(&frame);
                if (ret < 0)
                    goto end;
            } else {
                av_frame_free(&frame);
            }
        } else {

            av_packet_rescale_ts(&packet,
                                 psctx->ifmt_ctx->streams[stream_index]->time_base,
                                 psctx->ofmt_ctx->streams[stream_index]->time_base);

            ret = av_interleaved_write_frame(psctx->ofmt_ctx, &packet);
            if (ret < 0)
                goto end;
        }
        av_packet_unref(&packet);
    }


    for (i = 0; i < psctx->ifmt_ctx->nb_streams; i++) {

        if (!psctx->filter_ctx[i].filter_graph)
            continue;
        ret = filter_encode_write_frame(NULL, i,psctx);
        if (ret < 0) {
			printf("%s %s  %s:%d   Flushing filter failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }

        ret = flush_encoder(i,psctx);
        if (ret < 0) {
			printf("%s %s  %s:%d   Flushing encoder failed\n",__DATE__,__TIME__,__FILE__,__LINE__);
            goto end;
        }
    }

    av_write_trailer(psctx->ofmt_ctx);
end:
    av_packet_unref(&packet);
    av_frame_free(&frame);
	if (psctx->ifmt_ctx!=NULL){
		for (i = 0; i < psctx->ifmt_ctx->nb_streams; i++) {
			avcodec_free_context(&psctx->stream_ctx[i].dec_ctx);
			if (psctx->ofmt_ctx && psctx->ofmt_ctx->nb_streams > i && psctx->ofmt_ctx->streams[i] && psctx->stream_ctx[i].enc_ctx)
				avcodec_free_context(&psctx->stream_ctx[i].enc_ctx);
			if (psctx->filter_ctx && psctx->filter_ctx[i].filter_graph)
				avfilter_graph_free(&psctx->filter_ctx[i].filter_graph);
		}
	}

    av_free(psctx->filter_ctx);
    av_free(psctx->stream_ctx);
	if (psctx->ofmt_ctx!=NULL)
	{
		if (psctx->ofmt_ctx->flags & AVFMT_FLAG_CUSTOM_IO){
			if(psctx->ofmt_ctx->pb!=NULL)
			{
				if (psctx->ofmt_ctx->pb->buffer!=NULL)
				{
					av_free(psctx->ofmt_ctx->pb->buffer);
				}
				av_free(psctx->ofmt_ctx->pb);
				psctx->ofmt_ctx->pb = NULL;
			}
		}
	}
    if (psctx->ifmt_ctx!=NULL)
	{
		if (psctx->ifmt_ctx->flags & AVFMT_FLAG_CUSTOM_IO){
			av_free(psctx->ifmt_ctx->pb->buffer);
			av_free(psctx->ifmt_ctx->pb);
			psctx->ifmt_ctx->pb = NULL;
		}
	}

    avformat_close_input(&psctx->ifmt_ctx);
    avformat_free_context(psctx->ofmt_ctx);
    if (ret < 0)
		printf("%s %s  %s:%d   Error occurred: %s\n",__DATE__,__TIME__,__FILE__,__LINE__,av_err2str(ret));

    return ret ? 1 : 0;
}


*/
import "C"
import "unsafe"
import _ "fmt"
import _ "os"
import _ "strconv"

func FFInit() {
	C.av_register_all()
	C.avfilter_register_all()
	C.av_log_set_level(16)
}

func FFscale(data []byte) (resized []byte) {

	cdatail := C.int(len(data))
	cdataol := C.int(0)
	cdata := unsafe.Pointer(&data[0])
	cresizeData := C.ffprocess(cdatail, cdata, &cdataol)
	if cresizeData != nil && int(cdataol) > 0 {
		resized = C.GoBytes(unsafe.Pointer(cresizeData), cdataol)
		if cresizeData != cdata {
			C.free(cresizeData)
		}
	} else {
		return nil
	}
	return resized
}
