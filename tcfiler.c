/* 
 * File:   tcfiler.c
 * Author: evg
 *
 * Created on July 9, 2010, 1:32 AM
 */

#include <tcutil.h>
#include <tchdb.h>
#include <tcrdb.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <glob.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <sys/fcntl.h>

/*
 * 
 */

TCHDB *hdb;
TCRDB *tdb;
TCXSTR *db_file, *tdb_host;
int tdb_port;

bool use_cabinet_lib;

bool open_cabinet_db(int64_t bnum, bool optimize) {

    int ecode;

    if (optimize) fprintf(stdout, "Opening database optimized for %d items\n", bnum);

    /* tune up the db*/
    if (optimize && !tchdbtune(hdb, bnum, -1, -1, HDBTLARGE)) {
        ecode = tchdbecode(hdb);
        fprintf(stdout, "Failed to tune up database, error: %s\n", tchdberrmsg(ecode));
        return false;
    }

    /* open the database */
    if (!tchdbopen(hdb, tcxstrptr(db_file), HDBOWRITER | HDBOCREAT)) {
        ecode = tchdbecode(hdb);
        fprintf(stdout, "Failed to open database, error: %s\n", tchdberrmsg(ecode));
        return false;
    }

    return true;
}

bool open_tyrant_db() {

    int ecode;


    /* open the database */
    if (!tcrdbopen(tdb, tcxstrptr(tdb_host), tdb_port)) {
        ecode = tchdbecode(tdb);
        fprintf(stdout, "Failed to open database, error: %s\n", tcrdberrmsg(ecode));
        return false;
    }

    return true;

}

void close_db(){
    if(use_cabinet_lib){ 
        tchdbclose(hdb);
        tchdbdel(hdb);
    } else{ 
        tcrdbclose(tdb);
        tcrdbdel(tdb);
    }
}

bool key_exists(const char *key) {

    if (use_cabinet_lib) return tchdbvsiz2(hdb, key) > 0;
    else return tcrdbvsiz2(tdb, key) > 0;

}

char *get_file_name(char *path, size_t path_size) {

    if (NULL == path || 0 == path_size) return 0;

    char *file_name;
    file_name = path;
    char *c;
    char *fend = path + path_size - 1;
    for (c = fend; *c; c--) {

        if ('/' == *c) {
            file_name = c + 1;
            break;
        }
    }

    return file_name;
}

int unpack_files(char *root_dir, bool verbose, bool test) {

    char *key;
    void *value;
    int fd, count, key_size, val_size;


    /* traverse records */
    count = 0;
    tchdbiterinit(hdb);
    while ((key = tchdbiternext2(hdb)) != NULL) {
        key_size = (int) strlen(key);

        if (verbose || test) fprintf(stdout, "Extracting file: %s\n", key);

        value = tchdbget(hdb, key, key_size, &val_size);
        if (value && !test) {
            if (-1 == (fd = open(key, O_WRONLY | O_CREAT | O_TRUNC, 0644))) {
                fprintf(stdout, "***Failed open file %s\n", key);
                return 1;
            }

            write(fd, value, val_size);
            close(fd);
            free(value);
        }
        free(key);
        count++;
    }

    return count;
}

int pack_file(char *file, TCXSTR *root_key, bool resume) {

    struct stat st;
    int ecode;

    TCXSTR *key;
    key = tcxstrdup(root_key);

    size_t file_path_len;
    file_path_len = strlen(file);


    char *file_name;
    file_name = get_file_name(file, file_path_len);

    tcxstrcat2(key, file_name);

    if (resume && key_exists(tcxstrptr(key))) {
        fprintf(stdout, "already exists");
        return;
    }



    if (-1 == stat(file, &st)) {
        return 1;
    }

    if (!S_ISREG(st.st_mode)) {
        fprintf(stdout, "Not regular file: %s", file);
        return 2;
    }

    int fd;
    if (-1 == (fd = open(file, O_RDONLY))) {
        fprintf(stdout, "***Failed open file %s", file);
        return 1;
    }

    void *fmap;
    if (MAP_FAILED == (fmap = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0))) {
        fprintf(stdout, "mmaping failed for %s", file);

        close(fd);
        return -1;
    }

    /* store records */
    if (use_cabinet_lib) {
        if (!tchdbput(hdb, tcxstrptr(key), tcxstrsize(key), fmap, st.st_size)) {
            ecode = tchdbecode(hdb);
            fprintf(stdout, "put error: %s", tchdberrmsg(ecode));
        }
    } else {
        if (!tcrdbput(tdb, tcxstrptr(key), tcxstrsize(key), fmap, st.st_size)) {
            ecode = tcrdbecode(tdb);
            fprintf(stdout, "put error: %s", tcrdberrmsg(ecode));
        }


    }

    fprintf(stdout, "%d bytes", st.st_size);

    munmap(fmap, st.st_size);
    close(fd);
    tcxstrdel(key);
}

int main(int argc, char **argv) {

    TCXSTR *pattern, *key_root;
    int ecode;
    bool verbose, test, create, extract, optimize, resume;
    int o;

    verbose = test = create = extract = optimize = resume = false;
    pattern = tcxstrnew();
    key_root = tcxstrnew();
    db_file = tcxstrnew();
    tdb_host = tcxstrnew();

    tdb_port = 0;


    while (-1 != (o = getopt(argc, argv, "cxtvrof:p:k:H:P:"))) {

        switch (o) {
            case 'f':
                use_cabinet_lib = true;
                tcxstrcat2(db_file, optarg);

                /* create the object */
                hdb = tchdbnew();
                break;
            case 'H':
                use_cabinet_lib = false;
                tcxstrcat2(tdb_host, optarg);

                tdb = tcrdbnew();
                break;
            case 'P':
                tdb_port = strtol(optarg, NULL, 0);
                break;
            case 'k':
                tcxstrcat2(key_root, optarg);

                // add trailing slash if needed
                char *last_c;
                last_c = tcxstrptr(key_root) + tcxstrsize(key_root) - 1;
                if (*last_c != '/') {
                    tcxstrcat2(key_root, "/");
                }
                break;
            case 'p':
                tcxstrcat2(pattern, optarg);
                break;
            case 'v':
                verbose = true;
                break;
            case 't':
                test = true;
                break;
            case 'c':
                create = true;
                break;
            case 'x':
                extract = true;
                break;
            case 'r':
                resume = true;
                break;
            case 'o':
                optimize = true;
            case '?':
            default:
                break;
        }
    }

    if (!create && !extract) {
        fprintf(stdout, "No action specifed. Use -c to create DB and -x to extract files from dbfile.tch\n");
        return 1;
    }

    if ((use_cabinet_lib && NULL == hdb)
            ||
            (!use_cabinet_lib && NULL == tdb)) {
        fprintf(stdout, "No database specifed. Use -f dbfile.tch\n");
        return 1;
    }

    if (0 == tcxstrsize(pattern)) {
        fprintf(stdout, "No pattern is given. Using *. Use -p <glob_pattern> to override\n");
        tcxstrcat2(pattern, "*");
    }

    if (create) {
        glob_t gtree;
        glob(tcxstrptr(pattern), GLOB_NOSORT, NULL, &gtree);
        size_t found = gtree.gl_pathc;

        if (use_cabinet_lib && !open_cabinet_db((int64_t) found, optimize)) return 2;
        if (!use_cabinet_lib && !open_tyrant_db()) return 3;

        int i;
        for (i = 0; i < found; i++) {

            char * fname = gtree.gl_pathv[i];

            if (verbose || test) fprintf(stdout, "\n%d of %d - packing file: %s ...", i, found, fname);

            if (!test) pack_file(fname, key_root, resume);
        }

        fprintf(stdout, "Finished. Processed %d items\n", (int) found);
        globfree(&gtree);
    } else if (extract) {
        if (!open_cabinet_db(0, false)) return 2;

        int count;
        count = unpack_files(NULL, verbose, test);
        fprintf(stdout, "Finished. Processed %d items\n", count);
    }

    /* close the database */
    close_db();

    /* delete the objects */
    tcxstrdel(pattern);
    tcxstrdel(key_root);
    
    return 0;
}

