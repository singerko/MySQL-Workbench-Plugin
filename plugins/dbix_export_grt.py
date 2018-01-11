import re
import os
import StringIO
from path import path
from pprint import pprint
import inflect
import json

from wb import *

import grt
import mforms

from grt.modules import Workbench
from workbench.ui import WizardForm, WizardPage
from mforms import newButton, newCodeEditor, FileChooser

ModuleInfo = DefineModule(name = "DBIxExport", author = "Martin Spevak", version = "1.0")

#default values
project_path  = '/tmp/DBIx/Result'
package = 'DBIx::Result'
space = '    '

@ModuleInfo.plugin("my.plugin.export_dbix_file", caption = "Export DBIx files", input = [wbinputs.currentCatalog()], pluginMenu = "Catalog")
@ModuleInfo.export(grt.INT, grt.classes.db_Catalog)
def export_dbix_file(cat):
    global project_path, package

    out = StringIO.StringIO()

    for schema in [(s, s.name == 'main') for s in cat.schemata]:
        #process project settings
        schema_config = json.loads(schema[0].comment)
        project_path = schema_config['project_path']
        package = schema_config['package']
        #export schema
        export_schema(out, schema[0], schema[1])

    sql_text = out.getvalue()
    out.close()
    wizard = ExportSQLiteWizard(sql_text)
    wizard.run()

    return 0

def print_fk_columns(columns):
    for i, column in enumerate(columns):
        return column.name
    return 'NaN'

def safe_file_name(ident):
    """Create safe filename from identifer"""

    def repl(c):
        return ["%%%02x" % c for c in bytearray(c, 'ascii')]

    return re.sub(r'[/\:*?"<>|%]', repl, ident)

def dq(ident):
    """Double quote identifer, replacing " by "" """

    return '"' + re.sub(r'"', '""', ident) + '"'

def table_code_name(table_name):
    """Returns table name code (special characters and spaces are replaced)"""
    return re.sub(r'\s+', '_', table_name)

def attr_from_comment(comment, attr_name):
    """get attribute value from comment, comment filed is in JSON format"""
    try:
        j = json.loads(comment)
        return j[attr_name]
    except Exception, error:
        print 'invalid json: ' + str(error) + '\n'
        print comment + '\n'
        return None

def model_name(table_name):
    table_name = re.sub(r'_', ' ', table_name)
    return re.sub(r'\s+', '', table_name.title())

def export_table(db_name, schema, tbl):
    """Export one table into file"""

    if len(tbl.columns) == 0:
        return

    out = StringIO.StringIO()

    p = inflect.engine();
    out.write('use utf8;\n')
    out.write('package ' + package + '::%s;\n\n' % model_name(tbl.name))
    out.write(
        'use strict;\n'
        'use warnings;\n\n'
        'use Moose;\n'
        'use MooseX::NonMoose;\n'
        'use MooseX::MarkAsMethods autoclean => 1;\n'
        "extends 'DBIx::Class::Core';\n\n"
        '__PACKAGE__->load_components("InflateColumn::DateTime");\n'
    )

    out.write('__PACKAGE__->table("%s");\n' % tbl.name)
    out.write('\n')
    out.write('__PACKAGE__->add_columns(\n');

    primary_key = [i for i in tbl.indices if i.isPrimary == 1]
    primary_key = primary_key[0] if len(primary_key) > 0 else None

    additional = {}
    additional['unique'] = []
    additional['references'] = []

    pk_column = None
    if primary_key and len(primary_key.columns) == 1:
        pk_column = primary_key.columns[0].referencedColumn

    col_comment = ''
    for i, column in enumerate(tbl.columns):
        check, sql_column_type, flags = '', None, None
        if column.simpleType:
            sql_column_type = column.simpleType.name
            flags = column.simpleType.flags
        else:
            sql_column_type = column.userType.name
            flags = column.flags
        length = column.length

        # For INTEGER PRIMARY KEY column to become an alias for the rowid
        # the type needs to be "INTEGER" not "INT"
        # we fix it for other columns as well
        if sql_column_type == 'Image':
            sql_column_type = 'blob'
        if sql_column_type == 'FLOAT':
            sql_column_type = 'float'
        if sql_column_type == 'DATE':
            sql_column_type = 'date'
        if sql_column_type == 'DATETIME':
            sql_column_type = 'datetime'
        if 'INT' in sql_column_type or sql_column_type == 'LONG' or sql_column_type == 'INTEGER':
            sql_column_type = 'int'
            length = -1
            # Check flags for "unsigned"
            if 'UNSIGNED' in column.flags:
                check = dq(column.name) + '>=0'
        # We even implement ENUM (because we can)
        if sql_column_type == 'ENUM':
            sql_column_type = 'varchar'
            if column.datatypeExplicitParams:
                check = (dq(column.name) + ' IN' +
                         column.datatypeExplicitParams)
        if sql_column_type == 'TEXT' or sql_column_type == 'Email':
            sql_column_type = 'varchar'

        column_fkey = None
        for fkey in tbl.foreignKeys:
            for tmp_col in fkey.columns:
                if tmp_col.name == column.name:
                    column_fkey = fkey
            if column_fkey:
                break

        args = {}
        arr_args = []

        args['data_type'] = "'" + sql_column_type + "'"
        out.write(space + "'" + column.name + "',\n" + space + '{')
        if column_fkey:
            additional['references'].append(
                '__PACKAGE__->belongs_to(\n'
                '  "' + column_fkey.referencedTable.name + '",\n'
                '  "' + package + '::' + model_name(column_fkey.referencedTable.name) + '",\n'
                '  { "foreign.' + column.name + '" => "self.' + column.name +  '" },\n'
                '  { cascade_copy => 0, cascade_delete => 0 },\n'
                ');\n'
            )
        else:
            if column == pk_column:
                args['is_auto_increment'] = 1
                additional['primary_key'] = column.name
            elif sql_column_type == 'varchar':
                if length > 0:
                    args['size'] = length

        # Check for NotNull
        args['is_nullable'] = 0 if (column.isNotNull) else 1

        #Put non-primary, UNIQUE Keys in CREATE TABLE as well (because we can)
        for index in tbl.indices:
            if index != primary_key and index.indexType == 'UNIQUE':
                col_comment = ''
                additional['unique'].append({name: index.name, val: print_index_columns(index)})

        for key in sorted(list(args.keys())):
            arr_args.append(str(key) + ' => ' + str(args[key]))

        out.write(', '.join(arr_args))
        out.write('},\n')

    out.write(');\n\n')
    out.write('__PACKAGE__->set_primary_key("%s");\n' % additional['primary_key'])
    for index in additional['unique']:
        out.write('__PACKAGE__->add_unique_constraint("%s", [%s]);\n' % (index.name, index.val))

    for ref in additional['references']:
        out.write(ref)

    for other_tbl in schema.tables:
        if other_tbl.name == tbl.name:
            continue
        has_foreign_key = 0
        for fkey in other_tbl.foreignKeys:
            if fkey.referencedTable.name == tbl.name:
                has_foreign_key = 1
        if has_foreign_key > 0:
            out.write(
                '__PACKAGE__->has_many(\n'
                '  "' + p.plural(other_tbl.name) + '",\n'
                '  "' + package + '::' + model_name(other_tbl.name) + '",\n'
                '  { "foreign.' + print_fk_columns(fkey.columns) + '" => "self.' + print_fk_columns(fkey.columns) + '" },\n'
                '  { cascade_copy => 0, cascade_delete => 0 },\n'
                ');\n'
            )
            for next_tbl in schema.tables:
                if next_tbl.name == tbl.name or next_tbl.name == other_tbl.name:
                    continue
                for fkey in other_tbl.foreignKeys:
                    if fkey.referencedTable.name == next_tbl.name:
                        out.write(
                            '__PACKAGE__->many_to_many(' 
                            '"' + p.plural(next_tbl.name) + '''" => "''' + p.plural(other_tbl.name) + '",'
                            '"' + next_tbl.name + '"'
                            ');\n'
                        )

    out.write('\n\n')

    filename = project_path + '/' + model_name(tbl.name) + '.pm'
    print "filename: " + filename + "\n";

    if os.path.exists(filename):
        text = path(filename).bytes()
        pos  = text.find('#>>> content should be modified\n')
        if pos > 0:
            text = text[pos:]
            text = out.getvalue() + text
        else:
            out.write('#>>> content should be modified\n\n')
            out.write('#<<< content should be modified\n\n')
            out.write('1;')
            text = out.getvalue()
    else:
        out.write('#>>> content should be modified\n\n')
        out.write('#<<< content should be modified\n\n')
        out.write('1;')
        text = out.getvalue()

    try:
        print "trying to write: " + filename + "\n";
        with open(filename, 'w+') as f:
            f.write(text)
    except IOError as e:
        print 'error'

    out.close()

def export_schema(out, schema, is_main_schema):
    """Export all tables in schema"""

    print "project_path(sub): " + project_path + "\n";
    if len(schema.tables) == 0:
        return

    db_name = ''
    if not is_main_schema:
        db_name = dq(schema.name) + '.'

    unordered = {t.name: t for t in schema.tables}
    for tbl in unordered.values():
       export_table(db_name, schema, tbl)

class ExportSQLiteWizard_PreviewPage(WizardPage):
    def __init__(self, owner, sql_text):
        WizardPage.__init__(self, owner, 'DBIx export done.')

    def create_ui(self):
        button_box = mforms.newBox(True)
        button_box.set_padding(8)

        self.content.add_end(button_box, False, False)

class ExportSQLiteWizard(WizardForm):
    def __init__(self, sql_text):
        WizardForm.__init__(self, None)

        self.set_name('dbix_export_wizard')
        self.set_title('DBIx Export Wizard')

        self.preview_page = ExportSQLiteWizard_PreviewPage(self, sql_text)
        self.add_page(self.preview_page)
