# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
# Copyright (C) 2026 ScyllaDB

"""Verify documentation-time assembly of the server's Swagger fragments."""

import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scylladb_swagger import SwaggerProcessor


class SwaggerProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.source = root / 'api' / 'api-doc'
        shutil.copytree(Path(__file__).resolve().parents[3] / 'api' / 'api-doc', self.source)
        self.output = root / 'html' / '_static' / 'api' / 'api-doc'
        self.app = SimpleNamespace(
            srcdir=root,
            config=SimpleNamespace(scylladb_swagger_origin_api='api'),
            builder=SimpleNamespace(outdir=root / 'html', templates=object()),
        )

    def test_standalone_specs_and_legacy_files(self) -> None:
        """Client Routes and Metrics resolve their models; Swagger 1.2 stays byte-identical."""
        SwaggerProcessor().run(self.app)
        for name, path in [('client_routes', '/v2/client-routes'), ('metrics', '/v2/metrics-config/')]:
            with self.subTest(api=name):
                spec = json.loads((self.output / f'{name}.json').read_text())
                self.assertEqual(spec['swagger'], '2.0')
                self.assertEqual(spec['host'], '127.0.0.1:10000')
                self.assertIn('get', spec['paths'][path])
                self.assertIn('post', spec['paths'][path])
                models = spec['definitions']
                self.assertIn('ErrorModel', models)
                for schema in spec['paths'][path]['get']['responses'].values():
                    model = schema['schema'].get('items', schema['schema'])['$ref'].removeprefix('#/definitions/')
                    self.assertIn(model, models)
                self.assertIn(name + ('_entry' if name == 'client_routes' else '_config'), models)
        self.assertIn('delete', json.loads((self.output / 'client_routes.json').read_text())['paths']['/v2/client-routes'])
        for source in self.source.glob('*.json'):
            if source.name not in ('client_routes.json', 'metrics.json'):
                self.assertEqual(source.read_bytes(), (self.output / source.name).read_bytes())

    def test_rebuild_refreshes_specs_and_legacy_files(self) -> None:
        """An existing output directory must not hide edits to either API format."""
        SwaggerProcessor().run(self.app)
        for name, old_text in [('client_routes', 'List all client route entries'), ('raft', 'Triggers snapshot creation')]:
            source = self.source / f'{name}.json'
            text = source.read_text()
            self.assertIn(old_text, text)
            source.write_text(text.replace(old_text, 'Updated description'))
        definitions = self.source / 'client_routes.def.json'
        text = definitions.read_text()
        self.assertIn('Private connection identifier', text)
        definitions.write_text(text.replace('Private connection identifier', 'Updated connection identifier'))
        SwaggerProcessor().run(self.app)
        spec = json.loads((self.output / 'client_routes.json').read_text())
        self.assertTrue(spec['paths']['/v2/client-routes']['get']['description'].startswith('Updated description'))
        self.assertTrue(spec['definitions']['client_routes_entry']['properties']['connection_id']['description'].startswith('Updated'))
        self.assertEqual((self.source / 'raft.json').read_bytes(), (self.output / 'raft.json').read_bytes())

    def test_rebuild_removes_obsolete_assets(self) -> None:
        """Deleted source files must disappear from an existing HTML build."""
        SwaggerProcessor().run(self.app)
        removed = ['metrics.json', 'metrics.def.json', 'raft.json']
        for name in removed:
            (self.source / name).unlink()
        SwaggerProcessor().run(self.app)
        for name in removed:
            with self.subTest(asset=name):
                self.assertFalse((self.output / name).exists())

    def test_invalid_fragment_fails_build(self) -> None:
        """Malformed source data must fail the build instead of publishing a broken specification."""
        (self.source / 'client_routes.def.json').write_text('invalid')
        with self.assertRaises(json.JSONDecodeError):
            SwaggerProcessor().run(self.app)

    def test_non_html_builder_skips_assets(self) -> None:
        """Builders without HTML templates do not need Swagger assets."""
        self.app.builder.templates = None
        SwaggerProcessor().run(self.app)
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
