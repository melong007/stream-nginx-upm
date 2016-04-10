#include <ngx_config.h>
#include <ngx_core.h>
#include <nginx.h>
#include <ngx_stream.h>
#include <ngx_http.h>
#include <ngx_log.h>

#include "cJSON/cJSON.h"



//static char  ngx_http_proxy_version[] = " HTTP/1.0" CRLF;
static char  ngx_stream_upm_http_version_11[] = " HTTP/1.1" CRLF;

typedef struct {
    ngx_msec_t                       connect_timeout;   
    ngx_msec_t                       timeout;
    ngx_msec_t                       next_upstream_timeout;
    size_t                           buffer_size;
    size_t                           upload_rate;
    size_t                           download_rate;
    ngx_uint_t                       next_upstream_tries;
    ngx_flag_t                       next_upstream;
    ngx_flag_t                       proxy_protocol;
    ngx_addr_t                      *local;

    ngx_str_t                       *um_server;
    ngx_int_t                       interval;
    ngx_url_t                       *um_url;
    ngx_flag_t                       request_prepared;
    ngx_pool_t                      *pool;
    ngx_str_t                        method;

    ngx_stream_session_t            *fake_session;
    ngx_stream_upstream_srv_conf_t  *upstream;  //the configed upstream server 
} ngx_stream_upm_main_conf_t;


typedef enum {
    CONNECT = 1,
    CREATE_REQUEST,
    SEND_REQUEST,
    READ_AND_PARSE_HEADER,
    READ_AND_PARSE_RESPONSE,
    PROCESS,
} ngx_stream_upm_comet_stat_t;


typedef enum {
    ERR_STATUS_LINE = 0,
    ERR_HEADER,
    ERR_CONTENT_LENGTH,
    ERR_JSON,
} ngx_stream_upm_err_t;


typedef struct {
    ngx_str_t   name;   //ip:port
    ngx_str_t   host;
    ngx_int_t   port;
    
    ngx_int_t   fail_timeout;
    ngx_int_t   max_fail;
    ngx_int_t   weight;

    unsigned    down:1;
    unsigned    backup:1;
} ngx_stream_upm_service_inst_t;

typedef struct {
    ngx_str_t        service_name;
    ngx_array_t     *insts; 
} ngx_stream_upm_service_t;


typedef struct {
    unsigned            http_minor:16;
    unsigned            http_major:16;
    
    ngx_int_t           state;
    ngx_http_status_t   status;

} ngx_stream_upm_resp_status_t;


typedef struct {
    ngx_uint_t                        state;

    ngx_uint_t                        header_hash;
    ngx_uint_t                        lowcase_index;
    u_char                            lowcase_header[NGX_HTTP_LC_HEADER_LEN];

    u_char                           *header_name_start;
    u_char                           *header_name_end;
    u_char                           *header_start;
    u_char                           *header_end;
    

    ngx_flag_t                        invalid_header;
    ngx_stream_session_t             *s;
} ngx_stream_upm_resp_header_ctx_t;

static char * ngx_stream_upm_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
ngx_int_t ngx_stream_upm_init_worker(ngx_cycle_t *cycle);
static void * ngx_stream_upm_create_main_conf(ngx_conf_t *cf);
static char * ngx_stream_upm_init_main_conf(ngx_conf_t *cf, void *conf);
void ngx_stream_upm_empty_handler(ngx_event_t *ev);

ngx_int_t  ngx_stream_upm_connect(ngx_stream_session_t *s);
ngx_int_t  ngx_stream_upm_create_request(ngx_stream_session_t *s);
ngx_int_t  ngx_stream_upm_send_request(ngx_stream_session_t *s);
ngx_int_t  ngx_stream_upm_read_and_parse_header(ngx_stream_session_t *s);
//ngx_int_t  ngx_stream_upm_process_response(ngx_stream_session_t *s);

static void ngx_stream_upm_upstream_handler(ngx_event_t *ev);

ngx_int_t ngx_stream_upm_parse_status_line(ngx_stream_session_t *s, 
                                            ngx_buf_t *b,
                                            ngx_stream_upm_resp_status_t *urs);

typedef void (*ngx_stream_upm_event_handler_pt)(ngx_stream_session_t *s);
ngx_int_t ngx_stream_upm_process_resp_header(ngx_stream_session_t *s);

ngx_int_t ngx_stream_upm_parse_header_line(ngx_stream_upm_resp_header_ctx_t *ctx, ngx_buf_t *b,
                                            ngx_uint_t allow_underscores);

ngx_int_t ngx_stream_upm_read_and_parse_response(ngx_stream_session_t *s);


typedef struct {
    ngx_int_t       state;
   
    ngx_int_t       (*connect)(ngx_stream_session_t *s);
    ngx_int_t       (*create_request)(ngx_stream_session_t *s);
    ngx_int_t       (*send_request)(ngx_stream_session_t *s);
    ngx_int_t       (*read_and_parse_header)(ngx_stream_session_t *s);
    ngx_int_t       (*read_and_parse_response)(ngx_stream_session_t *s);
    ngx_int_t       (*process_response)(ngx_stream_session_t *s);

    unsigned        connection_close:1;
    ngx_array_t    *resp_headers;
    ngx_int_t        content_length_n;
    ngx_flag_t       chunked;
    ngx_buf_t       *resp_body;
    ngx_pool_t      *pool;


    ngx_stream_upm_event_handler_pt  read_event_handler;
    ngx_stream_upm_event_handler_pt  write_event_handler;

    ngx_stream_upm_resp_status_t resp_status;
} ngx_stream_upm_ctx_t;

static ngx_command_t ngx_stream_upm_commands[] = {

    { ngx_string("upm_server"),
      NGX_HTTP_LOC_CONF|NGX_CONF_NOARGS,
      ngx_stream_upm_parse,
      0,
      0,
      NULL },

    ngx_null_command
};


static ngx_stream_module_t ngx_stream_upm_module_ctx = {
    NULL,                                   /* postconfiguration */

    ngx_stream_upm_create_main_conf,        /* create main configuration */
    ngx_stream_upm_init_main_conf,          /* init main configuration */

    NULL,                                   /* create server configuration */
    NULL,                                   /* merge server configuration */   

};

ngx_module_t ngx_stream_upm_module = {
    NGX_MODULE_V1,
    &ngx_stream_upm_module_ctx,             /* module context */
    ngx_stream_upm_commands,                /* module directives */
    NGX_STREAM_MODULE,                      /* module type */
    NULL,                                   /* init master */
    NULL,                                   /* init module */
    ngx_stream_upm_init_worker,             /* init process */
    NULL,                                   /* init thread */
    NULL,                                   /* exit thread */
    NULL,                                   /* exit process */
    NULL,                                   /* exit master */
    NGX_MODULE_V1_PADDING
};


static void * 
ngx_stream_upm_create_main_conf(ngx_conf_t *cf)
{
    ngx_stream_upm_main_conf_t   *conf = NULL;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_stream_upm_main_conf_t));

    if (conf == NULL) {
        return NULL;
    }

    conf->connect_timeout = NGX_CONF_UNSET_MSEC;
    conf->timeout = NGX_CONF_UNSET_MSEC;
    conf->next_upstream_timeout = NGX_CONF_UNSET_MSEC;
    conf->buffer_size = NGX_CONF_UNSET_SIZE;
    conf->upload_rate = NGX_CONF_UNSET_SIZE;
    conf->download_rate = NGX_CONF_UNSET_SIZE;
    conf->next_upstream_tries = NGX_CONF_UNSET_UINT;
    conf->next_upstream = NGX_CONF_UNSET;
    conf->proxy_protocol = NGX_CONF_UNSET;
    conf->local = NGX_CONF_UNSET_PTR;

    conf->um_server = NGX_CONF_UNSET_PTR;    
    conf->interval = NGX_CONF_UNSET;    
    conf->um_url = NGX_CONF_UNSET_PTR;    

    return conf;
}


static char *
ngx_stream_upm_init_main_conf(ngx_conf_t *cf, void *conf)
{
    ngx_stream_upm_main_conf_t *ummcf = conf;

    if (ummcf->connect_timeout == NGX_CONF_UNSET_MSEC) {
        ummcf->connect_timeout = 60000;
    }

    if (ummcf->timeout == NGX_CONF_UNSET_MSEC) {
        ummcf->timeout = 60000;
    }

    if (ummcf->next_upstream_timeout == NGX_CONF_UNSET_MSEC) {
        ummcf->next_upstream_timeout = 0;
    }

    if (ummcf->buffer_size == NGX_CONF_UNSET_SIZE) {
        ummcf->buffer_size = 16384;
    }

    if (ummcf->upload_rate == NGX_CONF_UNSET_SIZE) {
        ummcf->upload_rate = 0;
    }

    /*
    ngx_conf_merge_size_value(conf->upload_rate,
                              prev->upload_rate, 0);

    ngx_conf_merge_size_value(conf->download_rate,
                              prev->download_rate, 0);

    ngx_conf_merge_uint_value(conf->next_upstream_tries,
                              prev->next_upstream_tries, 0);

    ngx_conf_merge_value(conf->next_upstream, prev->next_upstream, 1);

    ngx_conf_merge_value(conf->proxy_protocol, prev->proxy_protocol, 0);

    ngx_conf_merge_ptr_value(conf->local, prev->local, NULL);
    */

    return NGX_CONF_OK;
}


static char *
ngx_stream_upm_parse(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{

    u_short                     port;
    ngx_int_t                   add = 0;
    ngx_url_t                   *u;
    ngx_str_t                   *value, *url;
    ngx_stream_upm_main_conf_t   *ummcf = conf;

    if (ummcf->upstream) {
        return "is duplicate";
    }

    if (cf->args->nelts != 2) {
        return "With Invalid command args";
    }

    value = cf->args->elts;

    url = &value[1];

    ummcf->um_url = ngx_pcalloc(cf->pool, sizeof(ngx_url_t));
    if (ummcf->um_url == NULL) {
        return NGX_CONF_ERROR;
    }
    u = ummcf->um_url;

    if (ngx_strncasecmp(url->data, (u_char *)"http://", 7) == 0) {
        add = 7;    
        port = 80;        
    } else {
        return "Invalid args, only support http://";
    }

    u->url.len = url->len - add;
    u->url.data = url->data + add;
    u->default_port = port;
    u->uri_part = 1;
    u->no_resolve = 1;

    ummcf->upstream = ngx_stream_upstream_add(cf, u, 0);
    if (ummcf->upstream == NULL) {
        return NGX_CONF_ERROR;
    }

    return NGX_CONF_OK;
}

/*static ngx_int_t
ngx_stream_upm_connect_handler(ngx_conf_t *cf)
{
    //connect to upstream manager server
    return NGX_OK; 
}
*/



void 
ngx_stream_upm_empty_handler(ngx_event_t *ev)
{
    ngx_log_error(NGX_LOG_ERR, ev->log, 0, "enter ngx_stream_upm_empty_handler");
    return;
}


ngx_int_t
ngx_stream_upm_create_request(ngx_stream_session_t *s)
{
    ngx_int_t                        len, uri_len;
    ngx_str_t                        method, uri;
    ngx_buf_t                       *request_buf, *b;
    //ngx_stream_upm_ctx_t            *ctx;
    //ngx_stream_upstream_t           *u;
    ngx_stream_upm_main_conf_t      *ummcf;

    ummcf = ngx_stream_get_module_main_conf(s, ngx_stream_upm_module);
    //u = s->upstream;
    
    if (ummcf->request_prepared) {
        return NGX_OK;
    }

    if (ummcf->method.len) {
        method = ummcf->method;
    } else {
        method.data = (u_char *)"GET";
        method.len = 3;
    }
    //ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);

    //GET xx_uri VERSION\r\n
    len = method.len + 1 + sizeof(ngx_stream_upm_http_version_11) - 1
          + sizeof(CRLF) - 1;
    
    uri_len = ummcf->um_url->uri.len;
    len += uri_len;
    uri = ummcf->um_url->uri;

    //Only one header:   "Connection: keep-alive\r\n";
    len += 24;
    
    //The end \r\n;
    len += 2;
    
    request_buf = &s->upstream->downstream_buf;
    b = request_buf;

    /* the request line */
    b->last = ngx_copy(b->last, method.data, method.len);
    *b->last++ = ' ';
    b->last = ngx_copy(b->last, uri.data, uri.len);

    /*default use the HTTP/1.1 */
    b->last = ngx_cpymem(b->last, ngx_stream_upm_http_version_11,
                             sizeof(ngx_stream_upm_http_version_11) - 1);
    
    b->last = ngx_copy(b->last, "Connection: keep-alive\r\n", 24);

    /* add "\r\n" at the header end */
    *b->last++ = CR; *b->last++ = LF;

    ngx_log_debug2(NGX_LOG_DEBUG_STREAM, r->connection->log, 0,
                   "stream upm header:%N\"%*s\"",
                   (size_t) (b->last - b->pos), b->pos);

    /*Currently without any body;*/
    b->flush = 1;
    return NGX_OK;
}

static void
ngx_stream_upm_upstream_handler(ngx_event_t *ev)
{
    ngx_connection_t        *c;
    ngx_stream_session_t    *s;
    ngx_stream_upm_ctx_t    *ctx;
    //ngx_stream_upstream_t   *u;

    c = ev->data;
    s = c->data;

    //u = s->upstream;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);
    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "stream upm upstream handler");


    if (ev->write) {
        ctx->write_event_handler(s);
    } else {
        ctx->read_event_handler(s);
    }

    ngx_http_run_posted_requests(c);
}


ngx_int_t
ngx_stream_upm_connect(ngx_stream_session_t *s)
{
    ngx_int_t                     rc;
    ngx_connection_t             *c, *pc;
    ngx_stream_upm_ctx_t         *ctx;
    ngx_stream_upstream_t        *u;
    //ngx_stream_upm_main_conf_t    *ummcf;

    //ummcf = ngx_stream_get_module_srv_conf(s, ngx_stream_upm_module);
    c = s->connection;

    u = s->upstream;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);
    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, c->log, 0, "upm connect: %i", rc);

    rc = ngx_event_connect_peer(&u->peer);
    if (rc == NGX_ERROR) {
        return rc;
    }

    if (rc == NGX_BUSY) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0, "no live upstreams");
        return NGX_ERROR;
    }

    if (rc == NGX_DECLINED) {
        //FIXME: just return NGX_ERROR;
        return NGX_ERROR;
    }

    /* rc == NGX_OK || rc == NGX_AGAIN || rc == NGX_DONE */

    pc = u->peer.connection;

    pc->data = s;
    pc->log = c->log;
    pc->pool = c->pool;
    pc->read->log = c->log;
    pc->write->log = c->log;

    pc->recv = ngx_recv;
    pc->send = ngx_send;
    pc->recv_chain = ngx_recv_chain;
    pc->send_chain = ngx_send_chain;

    pc->sendfile = 1;

    pc->read->handler = ngx_stream_upm_upstream_handler;
    pc->write->handler = ngx_stream_upm_upstream_handler;
    //ngx_add_timer(pc->write, ummcf->connect_timeout);

    ctx->state = CREATE_REQUEST;
    rc = ngx_stream_upm_create_request(s);
    if (rc != NGX_OK) {
        //return ngx_stream_upm_finalize(s); 
        return NGX_ERROR;
    }

    ctx->state = SEND_REQUEST;
    return ngx_stream_upm_send_request(s);
}


ngx_int_t
ngx_stream_upm_parse_status_line(ngx_stream_session_t *s, ngx_buf_t *b,
    ngx_stream_upm_resp_status_t *urs)
{
    u_char   ch;
    u_char  *p;
    enum {
        sw_start = 0,
        sw_h,
        sw_ht,
        sw_htt,
        sw_http,
        sw_first_major_digit,
        sw_major_digit,
        sw_first_minor_digit,
        sw_minor_digit,
        sw_status,
        sw_space_after_status,
        sw_status_text,
        sw_almost_done
    } state;

    state = sw_start;

    for (p = b->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        /* "http/" */
        case sw_start:
            switch (ch) {
            case 'h':
                state = sw_h;
                break;
            default:
                return NGX_ERROR;
            }
            break;

        case sw_h:
            switch (ch) {
            case 't':
                state = sw_ht;
                break;
            default:
                return NGX_ERROR;
            }
            break;

        case sw_ht:
            switch (ch) {
            case 't':
                state = sw_htt;
                break;
            default:
                return NGX_ERROR;
            }
            break;

        case sw_htt:
            switch (ch) {
            case 'p':
                state = sw_http;
                break;
            default:
                return NGX_ERROR;
            }
            break;

        case sw_http:
            switch (ch) {
            case '/':
                state = sw_first_major_digit;
                break;
            default:
                return NGX_ERROR;
            }
            break;

        /* the first digit of major http version */
        case sw_first_major_digit:
            if (ch < '1' || ch > '9') {
                return NGX_ERROR;
            }

            urs->http_major = ch - '0';
            state = sw_major_digit;
            break;

        /* the major http version or dot */
        case sw_major_digit:
            if (ch == '.') {
                state = sw_first_minor_digit;
                break;
            }

            if (ch < '0' || ch > '9') {
                return NGX_ERROR;
            }

            urs->http_major = urs->http_major * 10 + ch - '0';
            break;

        /* the first digit of minor http version */
        case sw_first_minor_digit:
            if (ch < '0' || ch > '9') {
                return NGX_ERROR;
            }

            urs->http_minor = ch - '0';
            state = sw_minor_digit;
            break;

        /* the minor http version or the end of the request line */
        case sw_minor_digit:
            if (ch == ' ') {
                state = sw_status;
                break;
            }

            if (ch < '0' || ch > '9') {
                return NGX_ERROR;
            }

            urs->http_minor = urs->http_minor * 10 + ch - '0';
            break;

        /* http status code */
        case sw_status:
            if (ch == ' ') {
                break;
            }

            if (ch < '0' || ch > '9') {
                return NGX_ERROR;
            }

            urs->status.code = urs->status.code * 10 + ch - '0';

            if (++urs->status.count == 3) {
                state = sw_space_after_status;
                urs->status.start = p - 2;
            }

            break;

        /* space or end of line */
        case sw_space_after_status:
            switch (ch) {
            case ' ':
                state = sw_status_text;
                break;
            case '.':                    /* iis may send 403.1, 403.2, etc */
                state = sw_status_text;
                break;
            case CR:
                state = sw_almost_done;
                break;
            case LF:
                goto done;
            default:
                return NGX_ERROR;
            }
            break;

        /* any text until end of line */
        case sw_status_text:
            switch (ch) {
            case CR:
                state = sw_almost_done;

                break;
            case LF:
                goto done;
            }
            break;

        /* end of status line */
        case sw_almost_done:
            urs->status.end = p - 1;
            switch (ch) {
            case LF:
                goto done;
            default:
                return NGX_ERROR;
            }
        }
    }

    b->pos = p;
    urs->state = state;

    return NGX_AGAIN;

done:

    b->pos = p + 1;

    if (urs->status.end == NULL) {
        urs->status.end = p;
    }

    urs->status.http_version = urs->http_major * 1000 + urs->http_minor;
    urs->state = sw_start;

    return NGX_OK;
}

static ngx_int_t
ngx_stream_upm_process_resp_status_line(ngx_stream_session_t *s)
{
    //size_t                  len;
    ngx_int_t               rc;
    ngx_stream_upm_ctx_t    *ctx;
    ngx_stream_upstream_t   *u;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);

    if (ctx == NULL) {
        return NGX_ERROR;
    }

    u = s->upstream;

    rc = ngx_stream_upm_parse_status_line(s, &u->downstream_buf, &ctx->resp_status);

    if (rc == NGX_AGAIN) {
        return rc;
    }

    if (rc == NGX_ERROR) {
        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                      "upstream sent no valid HTTP/1.0 header");
        //u->resp_status.status.code = NGX_HTTP_OK;
        return NGX_ERROR;
    }

    //len = ctx->resp_status.status.end - ctx->resp_status.status.start;

    /* Without need to Copy the data;
    u->headers_in.status_line.data = ngx_pnalloc(r->pool, len);
    if (u->headers_in.status_line.data == NULL) {
        return NGX_ERROR;
    }
    ngx_memcpy(u->headers_in.status_line.data, ctx->status.start, len);
    */

    ngx_log_debug1(NGX_LOG_DEBUG_STREAM, s->connection->log, 0,
                   "stream upm status %ui", ctx->resp_status.status.code); 

    if (ctx->resp_status.status.http_version < NGX_HTTP_VERSION_11) {
        ctx->connection_close = 1;
    }

    ctx->read_and_parse_header = ngx_stream_upm_process_resp_header;
    return ngx_stream_upm_process_resp_header(s);
}


ngx_int_t
ngx_stream_upm_process_resp_header(ngx_stream_session_t *s)
{
    ngx_int_t                           rc;
    ngx_array_t                         *resp_headers;
    ngx_table_elt_t                     *h;
    ngx_stream_upm_ctx_t                *ctx;
    ngx_stream_upstream_t               *u;
    //ngx_stream_upm_main_conf_t           *ummcf;
    //ngx_http_upstream_header_t          *hh;
    //ngx_stream_upstream_srv_conf_t      *umcf;
    ngx_stream_upm_resp_header_ctx_t     hctx;
    
    //ummcf = ngx_stream_get_module_srv_conf(s, ngx_stream_upm_module);
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);

    u = s->upstream;
    resp_headers = ctx->resp_headers;
    if (resp_headers == NULL) {
        resp_headers = ngx_array_create(ctx->pool, 4, sizeof(ngx_table_elt_t));
        if (resp_headers == NULL) {
            return NGX_ERROR;
        }
        ctx->resp_headers = resp_headers;
    }
    ngx_memzero(&hctx, sizeof(ngx_stream_upm_resp_header_ctx_t));

    for ( ;; ) {

        rc = ngx_stream_upm_parse_header_line(&hctx, &u->upstream_buf, 1);

        if (rc == NGX_OK) {

            /* a header line has been parsed successfully */

            h = ngx_array_push(resp_headers);
            if (h == NULL) {
                return NGX_ERROR;
            }

            h->hash = hctx.header_hash;

            h->key.len = hctx.header_name_end - hctx.header_name_start;
            h->value.len = hctx.header_end - hctx.header_start;

            h->key.data = hctx.header_name_start;
            h->value.data = hctx.header_start;

            /*No need to care lowcase_key
            if (h->key.len == r->lowcase_index) {
                ngx_memcpy(h->lowcase_key, r->lowcase_header, h->key.len);
            } else {
                ngx_strlow(h->lowcase_key, h->key.data, h->key.len);
            }*/

            ngx_log_debug2(NGX_LOG_DEBUG_STREAM, r->connection->log, 0,
                           "stream header: \"%V: %V\"",
                           &h->key, &h->value);

            if (h->key.len == 14 && ngx_strncmp(h->key.data, "Content-Length", h->key.len) == 0) {
                ctx->content_length_n = ngx_atoof(h->value.data, h->value.len);
            }

            if (h->key.len == 17 && ngx_strncmp(h->key.data, "Transfer-Encoding", h->key.len) == 0) {
                if (ngx_strlcasestrn(h->value.data, h->value.data + h->value.len,
                         (u_char *) "chunked", 7 - 1) != NULL) {
                    ctx->chunked = 1;
                }
            }
            continue;
        }

        if (rc == NGX_HTTP_PARSE_HEADER_DONE) {

            /* a whole header has been parsed successfully */
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, r->connection->log, 0,
                           "stream upm header done");

            /* Get content length */
            u = s->upstream;

            if (ctx->chunked) {
                ctx->content_length_n = -1;
            }

            /*
             * set u->keepalive if response has no body; this allows to keep
             * connections alive in case of r->header_only or X-Accel-Redirect
             */
            return NGX_DONE;
        }

        if (rc == NGX_AGAIN) {
            return NGX_AGAIN;
        }

        /* there was error while a header line parsing */

        ngx_log_error(NGX_LOG_ERR, s->connection->log, 0,
                      "upm upstream sent invalid header");

        return NGX_HTTP_UPSTREAM_INVALID_HEADER;
    }
}


ngx_int_t
ngx_stream_upm_parse_header_line(ngx_stream_upm_resp_header_ctx_t *ctx, ngx_buf_t *b,
    ngx_uint_t allow_underscores)
{
    u_char      c, ch, *p;
    ngx_uint_t  hash, i;
    enum {
        sw_start = 0,
        sw_name,
        sw_space_before_value,
        sw_value,
        sw_space_after_value,
        sw_ignore_line,
        sw_almost_done,
        sw_header_almost_done
    } state;

    /* the last '\0' is not needed because string is zero terminated */

    static u_char  lowcase[] =
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        "\0\0\0\0\0\0\0\0\0\0\0\0\0-\0\0" "0123456789\0\0\0\0\0\0"
        "\0abcdefghijklmnopqrstuvwxyz\0\0\0\0\0"
        "\0abcdefghijklmnopqrstuvwxyz\0\0\0\0\0"
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
        "\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

    state = ctx->state;
    hash = ctx->header_hash;
    i = ctx->lowcase_index;

    for (p = b->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        /* first char */
        case sw_start:
            ctx->header_name_start = p;
            ctx->invalid_header = 0;

            switch (ch) {
            case CR:
                ctx->header_end = p;
                state = sw_header_almost_done;
                break;
            case LF:
                ctx->header_end = p;
                goto header_done;
            default:
                state = sw_name;

                c = lowcase[ch];

                if (c) {
                    hash = ngx_hash(0, c);
                    ctx->lowcase_header[0] = c;
                    i = 1;
                    break;
                }

                if (ch == '_') {
                    if (allow_underscores) {
                        hash = ngx_hash(0, ch);
                        ctx->lowcase_header[0] = ch;
                        i = 1;

                    } else {
                        ctx->invalid_header = 1;
                    }

                    break;
                }

                if (ch == '\0') {
                    return NGX_HTTP_PARSE_INVALID_HEADER;
                }

                ctx->invalid_header = 1;

                break;

            }
            break;

        /* header name */
        case sw_name:
            c = lowcase[ch];

            if (c) {
                hash = ngx_hash(hash, c);
                ctx->lowcase_header[i++] = c;
                i &= (NGX_HTTP_LC_HEADER_LEN - 1);
                break;
            }

            if (ch == '_') {
                if (allow_underscores) {
                    hash = ngx_hash(hash, ch);
                    ctx->lowcase_header[i++] = ch;
                    i &= (NGX_HTTP_LC_HEADER_LEN - 1);

                } else {
                    ctx->invalid_header = 1;
                }

                break;
            }

            if (ch == ':') {
                ctx->header_name_end = p;
                state = sw_space_before_value;
                break;
            }

            if (ch == CR) {
                ctx->header_name_end = p;
                ctx->header_start = p;
                ctx->header_end = p;
                state = sw_almost_done;
                break;
            }

            if (ch == LF) {
                ctx->header_name_end = p;
                ctx->header_start = p;
                ctx->header_end = p;
                goto done;
            }

            /* IIS may send the duplicate "HTTP/1.1 ..." lines */
            if (ch == '/'
                //&& ctx->upstream
                && p - ctx->header_name_start == 4
                && ngx_strncmp(ctx->header_name_start, "HTTP", 4) == 0)
            {
                state = sw_ignore_line;
                break;
            }

            if (ch == '\0') {
                return NGX_HTTP_PARSE_INVALID_HEADER;
            }

            ctx->invalid_header = 1;

            break;

        /* space* before header value */
        case sw_space_before_value:
            switch (ch) {
            case ' ':
                break;
            case CR:
                ctx->header_start = p;
                ctx->header_end = p;
                state = sw_almost_done;
                break;
            case LF:
                ctx->header_start = p;
                ctx->header_end = p;
                goto done;
            case '\0':
                return NGX_HTTP_PARSE_INVALID_HEADER;
            default:
                ctx->header_start = p;
                state = sw_value;
                break;
            }
            break;

        /* header value */
        case sw_value:
            switch (ch) {
            case ' ':
                ctx->header_end = p;
                state = sw_space_after_value;
                break;
            case CR:
                ctx->header_end = p;
                state = sw_almost_done;
                break;
            case LF:
                ctx->header_end = p;
                goto done;
            case '\0':
                return NGX_HTTP_PARSE_INVALID_HEADER;
            }
            break;

        /* space* before end of header line */
        case sw_space_after_value:
            switch (ch) {
            case ' ':
                break;
            case CR:
                state = sw_almost_done;
                break;
            case LF:
                goto done;
            case '\0':
                return NGX_HTTP_PARSE_INVALID_HEADER;
            default:
                state = sw_value;
                break;
            }
            break;

        /* ignore header line */
        case sw_ignore_line:
            switch (ch) {
            case LF:
                state = sw_start;
                break;
            default:
                break;
            }
            break;

        /* end of header line */
        case sw_almost_done:
            switch (ch) {
            case LF:
                goto done;
            case CR:
                break;
            default:
                return NGX_HTTP_PARSE_INVALID_HEADER;
            }
            break;

        /* end of header */
        case sw_header_almost_done:
            switch (ch) {
            case LF:
                goto header_done;
            default:
                return NGX_HTTP_PARSE_INVALID_HEADER;
            }
        }
    }

    b->pos = p;
    ctx->state = state;
    ctx->header_hash = hash;
    ctx->lowcase_index = i;

    return NGX_AGAIN;

done:

    b->pos = p + 1;
    ctx->state = sw_start;
    ctx->header_hash = hash;
    ctx->lowcase_index = i;

    return NGX_OK;

header_done:

    b->pos = p + 1;
    ctx->state = sw_start;

    return NGX_HTTP_PARSE_HEADER_DONE;
}

ngx_int_t  
ngx_stream_upm_read_and_parse_header(ngx_stream_session_t *s)
{
    ngx_int_t                       n, rc;
    ngx_buf_t                       *b, tb;
    ngx_connection_t                *pc; 
    ngx_stream_upm_ctx_t            *ctx;
    ngx_stream_upstream_t           *u;
    ngx_stream_upm_main_conf_t       *ummcf;
 
    ummcf = ngx_stream_get_module_srv_conf(s, ngx_stream_upm_module);
    u = s->upstream; 
    pc = u->peer.connection;
    
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);

    if (u->upstream_buf.start == NULL) {
        tb.start = ngx_pcalloc(ctx->pool, ummcf->buffer_size * sizeof(char));
        if (tb.start == NULL) {
            return NGX_ERROR;
        }
        tb.end = tb.start + ummcf->buffer_size;
        tb.pos = tb.start;
        tb.last = tb.pos;
        u->upstream_buf = tb;
    }
    b = &u->upstream_buf;

    for ( ;; ) {
        n = pc->recv(pc, b->last, b->end - b->last);
        if (n == NGX_AGAIN) {
            if (ngx_handle_read_event(pc->read, 0) != NGX_OK) {
                return NGX_ERROR;
            }
            return NGX_AGAIN;
        }

        if (n == 0) {
            ngx_log_error(NGX_LOG_ERR, pc->log, 0,
                          "upm upstream prematurely closed connection");
        }

        if (n == NGX_ERROR || n == 0) {
            return NGX_ERROR;
        }

        b->last += n;
        rc = ctx->read_and_parse_header(s);

        if (rc == NGX_AGAIN) {

            if (u->upstream_buf.last == u->upstream_buf.end) {
                ngx_log_error(NGX_LOG_ERR, pc->log, 0,
                              "upm upstream sent too big header");
                return NGX_HTTP_UPSTREAM_FT_INVALID_HEADER;
            }
            continue;
        }
        
        if(rc == NGX_DONE) {
            ctx->state = READ_AND_PARSE_RESPONSE;
            return ngx_stream_upm_read_and_parse_response(s);
        }
        return rc;
    }
}


ngx_int_t
ngx_stream_upm_parse_resp_body(ngx_stream_session_t *s)
{
    int                              port, fail_timeout, max_fail, weight, ta[1024];
    char                            *upname, *host, *t;
    cJSON                           *jsroot, *service, *jinsts, *jinst, *tags;
    ngx_buf_t                       *b;
    ngx_uint_t                       i, j, p, st, sfound, ifound;
    ngx_conf_t                       cf;
    //ngx_pool_t                      *pool;
    ngx_array_t                     *servs, *upstreams;
    ngx_connection_t                *pc; 
    ngx_stream_upstream_t           *u;
    ngx_stream_upm_ctx_t            *ctx;
    ngx_stream_upm_service_t        *ums;
    ngx_stream_upm_main_conf_t       *ummcf;

    ngx_stream_upm_service_inst_t   *umsi;

    ngx_stream_upstream_init_pt      init;
    ngx_stream_upstream_server_t     *us;
    ngx_stream_upstream_srv_conf_t   *uscf;
    ngx_stream_upstream_main_conf_t  *umcf;
 
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);
    ummcf = ngx_stream_get_module_srv_conf(s, ngx_stream_upm_module);

    umcf = ngx_stream_get_module_main_conf(s, ngx_stream_upstream_module);
    upstreams = &umcf->upstreams;
        
    b = ctx->resp_body;
    u = s->upstream; 
    pc = u->peer.connection;
    
    /*
     start to parse the JSON body;
    */
    jsroot = cJSON_Parse((char *)b->start);
    if (jsroot == NULL) {
        ngx_log_error(NGX_LOG_ERR, pc->log, 0, "upm json parse failed");
        return ERR_JSON;
    }
    
    /*
     start to parse the JSON;
     */
    service = jsroot->child;  
    if (service == NULL) {
        ngx_log_error(NGX_LOG_ERR, pc->log, 0, "upm json with empty service list");
        return ERR_JSON;
    }

    /*service array */
    servs = ngx_array_create(ctx->pool, 16, sizeof(ngx_stream_upm_service_t));
    if (servs == NULL) {
        return NGX_ERROR;
    }

    while( service != NULL ) {
        //Got the service name;
        upname = service->string;
        
        //Got the service related instance;
        jinsts = service->child; 
        if (jinsts == NULL || jinsts->child == NULL) {
            ngx_log_error(NGX_LOG_ERR, pc->log, 0, "upm service: %s without any instance", upname);
        }

        ums = ngx_array_push(servs);
        if (ums == NULL) {
            return NGX_ERROR; 
        }

        ums->service_name.len = ngx_strlen(upname);
        ums->service_name.data = ngx_pcalloc(ctx->pool, ums->service_name.len);
        ngx_memcpy(ums->service_name.data, upname, ums->service_name.len);
        
        ums->insts = ngx_array_create(ctx->pool, 16, sizeof(ngx_stream_upm_service_inst_t));
        if (ums->insts == NULL) {
            return NGX_ERROR;
        }

        //travers all the instance
        jinst = jinsts;
        while (jinst != NULL) {

            //FIXME need check the Item isn't exists;
            host = cJSON_GetObjectItem(jinst, "Host")->valuestring;
            port = cJSON_GetObjectItem(jinst, "Port")->valueint;

            tags = cJSON_GetObjectItem(jinst, "Tags");
            
            fail_timeout = cJSON_GetObjectItem(tags, "fail_timeout")->valueint; 
            max_fail = cJSON_GetObjectItem(tags, "max_fails")->valueint; 
            weight = cJSON_GetObjectItem(tags, "weight")->valueint; 

        
            umsi = ngx_array_push(ums->insts);
            if (umsi == NULL) {
                return NGX_ERROR;
            }
            i = ngx_strlen(host); 

            //Here, ip:port, Max port is 65535
            umsi->name.data = ngx_pcalloc(ctx->pool, (i + 5) * sizeof(char));
            t = (char *)ngx_snprintf(umsi->name.data, i + 5, "%s:%d", (char *)host, port);
            umsi->name.len = t - (char *)umsi->name.data;

            umsi->host.data = (u_char *)host;
            umsi->host.len = i;
            umsi->port = port; 

            umsi->fail_timeout = fail_timeout; 
            umsi->max_fail = max_fail; 
            umsi->weight = weight; 
            jinst = jinst->next;
        }
        service = service->next;
    }

    //Process the upstream msg, check wether we need to update the server;

    cf.name = "ngx_stream_upm_module";

    //Here, we create a memory pool, Only use to the upstream init peer;
    cf.pool = ngx_create_pool(8192, pc->log);
    cf.module_type = NGX_STREAM_MODULE;
    cf.cmd_type = NGX_STREAM_SRV_CONF;
    cf.log = pc->log;

    if (ummcf->pool == NULL) {
        ummcf->pool = cf.pool;   
    } else {
        //pool = cf.pool;
        ummcf->pool = cf.pool;
    }

    for (i = 0; i < servs->nelts; i++) {
        ums = &((ngx_stream_upm_service_t *)(servs->elts))[i];
        sfound = 0;
        /*First: find the upstream name */
        uscf = NULL;
        for (j = 0; j < upstreams->nelts; j++) {
            uscf = &((ngx_stream_upstream_srv_conf_t *)upstreams->elts)[j];
            if (uscf->host.len == ums->service_name.len &&
                ngx_strncmp(uscf->host.data, ums->service_name.data, uscf->host.len) == 0)
            {
                sfound = 1;
            }
        }

        if (sfound == 1) {
            //reset the temporary array to zero;
            //the elt == 1, Means update;
            //the elt == 2, Means create;
            //the elt == 0, Means need set this server to down;
            memset(ta, 0, sizeof(ta));

            us = NULL;
            for (p = 0; p < ums->insts->nelts; p++) {
                umsi = &((ngx_stream_upm_service_inst_t *)ums->insts->elts)[p];
                ifound = 0;
                for (st = 0; st < uscf->servers->nelts; st++) {
                    us = &((ngx_stream_upstream_server_t *)uscf->servers->elts)[st];     
                    if(us->name.len == umsi->name.len && 
                       ngx_strncmp(us->name.data, umsi->name.data, us->name.len ) == 0) 
                    {
                        ifound = 1;
                    }
                }

                //Means server already exists;
                if (ifound == 1) {
                    us->weight = umsi->weight;
                    us->max_fails = umsi->max_fail;
                    us->fail_timeout = umsi->fail_timeout;
                    us->backup = umsi->backup; 
                    us->down = umsi->down; 
                    ta[st] = 1;
                //insert the server;
                } else {
                    us = ngx_array_push(uscf->servers);
                    us->weight = umsi->weight;
                    us->max_fails = umsi->max_fail;
                    us->fail_timeout = umsi->fail_timeout;
                    us->backup = umsi->backup; 
                    us->down = umsi->down; 
                    ta[st] = 2;
                }
            }

            for (st = 0; st < uscf->servers->nelts; st++) {
                us =  &((ngx_stream_upstream_server_t *)uscf->servers->elts)[st];
                if (ta[st] == 0) {
                    //FIXME: Only set this server to down, 
                    //       If the backend change very quikly, there are too many down server list;
                    us->down = 1; 
                }
            }

            /*Reinit the upstream servers*/
            init = uscf->peer.init_upstream ? uscf->peer.init_upstream:
                     ngx_stream_upstream_init_round_robin;
            if (init(&cf, uscf) != NGX_OK) {
                return NGX_ERROR;
            }
        } else {
            //TODO: doesn't support auto discovery the service name?
            //Need alloc the memory from the global;
            ngx_log_error(NGX_LOG_ERR, pc->log, 0, 
                          "config server return uninitilized usptreams: %V", &ums->service_name);
        }
    }
    /*Update the upstream weight*/
    return NGX_OK;
}


ngx_int_t
ngx_stream_upm_read_and_parse_response(ngx_stream_session_t *s)
{
    ngx_int_t                       n, rc;
    ngx_buf_t                       *b;
    ngx_connection_t                *pc; 
    ngx_stream_upm_ctx_t            *ctx;
    ngx_stream_upstream_t           *u;
    ngx_stream_upm_main_conf_t       *ummcf;
    
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);
    ummcf = ngx_stream_get_module_srv_conf(s, ngx_stream_upm_module);

    u = s->upstream;
    pc = u->peer.connection;
    
    //Allocate big enough buffer
    //Must be assure the content_length_n big than 2 * ummcf->buffer_size;
    b = ctx->resp_body;
    if (b->start == NULL)  {
        b->start = ngx_pcalloc(ctx->pool, 2 * ummcf->buffer_size);    
        b->pos = b->start;
        b->last = b->start;
        b->end = b->start + 2 * ummcf->buffer_size; 
    }
    
    for (;;) {

        n = pc->recv(pc, b->last, b->end - b->last);
        if (n == NGX_AGAIN) {
            if (ngx_handle_read_event(pc->read, 0) != NGX_OK) {
                return NGX_ERROR;
            }
            return NGX_AGAIN;
        }

        if (n == 0) {
            ngx_log_error(NGX_LOG_ERR, pc->log, 0,
                          "upm upstream prematurely closed connection while read the body");
        }

        if (n == NGX_ERROR || n == 0) {
            return NGX_ERROR;
        }

        b->last += n;
        if (b->last - b->pos == ctx->content_length_n) {
            rc = ngx_stream_upm_parse_resp_body(s);
        } else if (b->last - b->pos < ctx->content_length_n) {
            continue;
        } else {
            ngx_log_error(NGX_LOG_ERR, pc->log, 0,
                          "upm upstream send a response body more than the content_length");
            return NGX_ERROR;
        }
    }

    return rc;
}

ngx_int_t 
ngx_stream_upm_send_request(ngx_stream_session_t *s)
{
    ngx_int_t                       n;
    ngx_buf_t                       *buf;
    ngx_connection_t                *pc; 
    ngx_stream_upm_ctx_t            *ctx;
    ngx_stream_upstream_t           *u;
    
    u = s->upstream;
    pc = u->peer.connection;

    buf = &u->downstream_buf;
    ctx = ngx_stream_get_module_ctx(s, ngx_stream_upm_module);
    if (ctx->state == SEND_REQUEST) {
        n = pc->send(pc, buf->pos, buf->last - buf->pos);
        if (n == NGX_ERROR) {
            return NGX_ERROR;
        }

        //all nginx error;
        if (n == buf->last - buf->pos) {
            ctx->state = READ_AND_PARSE_HEADER;
            //FIXME: need fix the steps
            return ngx_stream_upm_process_resp_status_line(s);
        } else {
            buf->pos += n;
        }
    } 
    return NGX_OK;
}

ngx_int_t
ngx_stream_upm_init_worker(ngx_cycle_t * cycle)
{
    u_char                          *p;
    //ngx_int_t                        rc;
    ngx_connection_t                *c;
    //ngx_connection_t                *pc;
    ngx_stream_upm_ctx_t            *ctx;
    ngx_stream_session_t            *fake_session;
    ngx_stream_upstream_t           *u;
    //ngx_peer_connection_t           *peer;
    ngx_stream_upm_main_conf_t      *ummcf;
    ngx_stream_upstream_srv_conf_t  *uscf; 

    //Fake a client session
    ummcf = ngx_stream_cycle_get_module_main_conf(cycle, ngx_stream_upm_module);
    ummcf->fake_session = ngx_pcalloc(cycle->pool, sizeof(ngx_stream_session_t));
    if (ummcf->fake_session == NULL) {
        return NGX_ERROR;    
    }

    fake_session = ummcf->fake_session;
    //Fake the client fd as 254
    c = ngx_get_connection(254, cycle->log);
    if (c == NULL) {
        return NGX_ERROR;
    }
    //Set the fake conn's base items
    c->pool = cycle->pool;
    c->log = cycle->log;

    fake_session->signature = NGX_STREAM_MODULE;
    fake_session->main_conf = ((ngx_stream_conf_ctx_t *)cycle->conf_ctx)->main_conf;
    fake_session->srv_conf = ((ngx_stream_conf_ctx_t *)cycle->conf_ctx)->srv_conf;

    fake_session->connection = c;
    c->data = fake_session;

    u = ngx_pcalloc(c->pool, sizeof(ngx_stream_upstream_t));
    if (u == NULL) {
        return NGX_ERROR;
    }
    fake_session->upstream = u;
    
    //Init the upm ctx;
    fake_session->ctx = ngx_pcalloc(cycle->pool, sizeof(void *) * ngx_stream_max_module);
    if (fake_session->ctx == NULL) {
        return NGX_ERROR;
    }
    ctx = ngx_pcalloc(cycle->pool, sizeof(ngx_stream_upm_ctx_t));
    ngx_stream_set_ctx(fake_session, ctx, ngx_stream_upm_module);
    
    ctx->state = CONNECT;
    ctx->connect = ngx_stream_upm_connect;
    ctx->create_request = ngx_stream_upm_create_request;
    ctx->send_request = ngx_stream_upm_send_request;
    ctx->read_and_parse_response = ngx_stream_upm_read_and_parse_response;
    //ctx->process_response = ngx_stream_upm_process_response;

    u->peer.log = cycle->log;
    u->peer.log_error = NGX_ERROR_ERR;
    //whitout need set the local
    u->peer.local = NULL;

    uscf = ummcf->upstream;
    if (uscf->peer.init(fake_session, uscf) != NGX_OK) {
        return NGX_ERROR;
    }

    u->peer.start_time = ngx_current_msec;

    if (ummcf->next_upstream_tries
        && u->peer.tries > ummcf->next_upstream_tries)
    {
        u->peer.tries = ummcf->next_upstream_tries;
    }
    u->start_sec = ngx_time();

    //May without need alloc the downstream_buf
    p = ngx_pnalloc(c->pool, ummcf->buffer_size);
    if (p == NULL) {
        return NGX_ERROR;
    }
    
    //web use the downstream_buf to buffer the request;
    u->downstream_buf.start = p;
    u->downstream_buf.end = p + ummcf->buffer_size;
    u->downstream_buf.pos = p;
    u->downstream_buf.last = p;

    c->write->handler = ngx_stream_upm_empty_handler;
    c->read->handler = ngx_stream_upm_empty_handler;

    return ngx_stream_upm_connect(fake_session);
}
